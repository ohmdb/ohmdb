package com.ohmdb.filestore;

/*
 * #%L
 * ohmdb-core
 * %%
 * Copyright (C) 2013 - 2014 Nikolche Mihajlovski
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.ohmdb.api.Db;
import com.ohmdb.api.TransactionListener;
import com.ohmdb.codec.StoreCodec;
import com.ohmdb.impl.OhmDBImpl;
import com.ohmdb.impl.OhmDBStats;
import com.ohmdb.util.Check;
import com.ohmdb.util.Errors;
import com.ohmdb.util.U;

/**
 * A block will be skipped if the first long number is negative!
 */
public class FileStore extends AbstractDataStore implements DataStore, Runnable {

	private static final String MARK = "OhmDB";

	private static final String MARK2 = "BEGIN";

	// 8B txOrder + 8B version + 4B size + 4B CRC
	public static final int BLOCK_OVERHEAD = 32;

	public static final int FIRST_CAP = 32;

	public static final int BLOCK_SIZE = BLOCK_OVERHEAD + FIRST_CAP;

	protected static final int BUF_SIZE = 5 * 1024 * 1024;

	protected static final int MAX_BLOCKS = BUF_SIZE / BLOCK_SIZE;

	protected static final int HEADER_SIZE = 1024;

	private static final int BASE_ADDRESS = HEADER_SIZE;

	private static final long TX_FREE_SPACE = -1;

	private static final int TX_MAX_COUNT = 100000;

	private final ByteBuffer BUF = ByteBuffer.allocateDirect(BUF_SIZE);

	private final ByteBuffer READ_BUF;

	private final byte[] READ_ITEM_ARR = new byte[BUF_SIZE];

	private final ByteBuffer READ_ITEM_BUF = ByteBuffer.wrap(READ_ITEM_ARR);

	private final ByteBuffer TX_BUF = ByteBuffer.allocateDirect(3 * 8);

	private final ByteBuffer BUF16 = ByteBuffer.allocateDirect(16);

	private final Zones2 zones = new Zones2();

	private final StoreInfo infos = new StoreInfo();

	private final String filename;

	private final StoreCodec<Object> valueCodec;

	// FIXME: check this when loading data
	private AtomicLong txCounter = new AtomicLong();

	private boolean firstTxCounter = true;

	private final Map<DbStats, Object> dbstats = new HashMap<DbStats, Object>();

	@SuppressWarnings("unused")
	private final OhmDBStats stats;

	private final Thread thread;

	int totalBlocks = 0;
	int errorBlocks = 0;

	private Queue<FilestoreTransaction> txs = new ArrayBlockingQueue<FilestoreTransaction>(TX_MAX_COUNT);

	// private Queue<FilestoreTransaction> txs = new
	// ConcurrentLinkedQueue<FilestoreTransaction>();

	private int aggregatedSize;

	private final List<KeyAndSize> aggregatedKeys = new ArrayList<KeyAndSize>(TX_MAX_COUNT);

	protected AtomicBoolean finished = new AtomicBoolean();

	private final RandomAccessFile file;

	private final WeakReference<Db> dbRef;

	@SuppressWarnings("unchecked")
	public FileStore(String filename, StoreLoader loader, StoreCodec<?> valueCodec, OhmDBStats stats, boolean loadOnly,
			WeakReference<Db> dbRef) {
		this.filename = filename;
		this.valueCodec = (StoreCodec<Object>) valueCodec;
		this.stats = stats;
		this.dbRef = dbRef;

		assert (BUF.capacity() % BLOCK_SIZE) == 0;

		File dbFile = new File(filename);
		if (dbFile.exists()) {
			int sizeKB = (int) (dbFile.length() / 1024);
			Check.state(sizeKB >= 0, "Database file is too big!");

			System.out.println(String.format("Loading database from: %s (%s KB)...", filename, sizeKB));

			this.READ_BUF = ByteBuffer.allocateDirect((sizeKB + 2) * 1024);

			long time = System.currentTimeMillis();
			loadData(loader);
			System.out.println(String.format("Database loaded in %s ms", System.currentTimeMillis() - time));
		} else {
			this.READ_BUF = null;
			System.out.println("Creating database: " + filename + "...");
		}

		if (!loadOnly) {
			try {
				this.file = new RandomAccessFile(filename, "rw");

				startWriting();
			} catch (IOException e) {
				throw Errors.rte("Cannot open file: " + filename, e);
			}

			this.thread = new Thread(this);
			thread.start();
		} else {
			this.thread = null;
			this.file = null;
		}

		System.out.println("Database is ready.");
	}

	private OhmDBImpl db() {
		return (OhmDBImpl) (dbRef != null ? dbRef.get() : null);
	}

	private void createDb() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(HEADER_SIZE);
		writeHeader(buf);
		buf.rewind();
		write(file, buf, HEADER_SIZE);
	}

	private void writeHeader(ByteBuffer buf) {
		buf.put(MARK.getBytes());
		buf.putInt(1); // file format version
		buf.putLong(0); // oddTx1
		buf.putLong(0); // oddTx2
		buf.putLong(0); // oddTx3
		buf.putLong(0); // evenTx1
		buf.putLong(0); // evenTx2
		buf.putLong(0); // evenTx3
		buf.position(HEADER_SIZE - MARK2.length());
		buf.put(MARK2.getBytes());
	}

	/**
	 * Format: OhmDB version = 1 last_transaction1 = 12345|12345|12345
	 * last_transaction2 = 12346|12346|12344 block_size = 40
	 */
	private void loadHeader(RandomAccessFile fc) throws IOException {
		dbstats.clear();

		ByteBuffer buf = ByteBuffer.allocateDirect(HEADER_SIZE);

		long n = 0;
		int read;
		do {
			read = fc.getChannel().read(buf);
			n += read;
		} while (read > 0); // fill the buffer

		assert n == HEADER_SIZE;

		Check.state(n == HEADER_SIZE, "The file doesn't have a valid OhmDB format!");

		dbstats.put(DbStats.HEADER_SIZE, n);

		buf.rewind();

		String mark = readStr(buf, MARK.length());
		Check.state(mark.equals(MARK), "The file doesn't have a valid OhmDB format (incorrect mark)!");

		int fileVersion = buf.getInt();
		Check.state(fileVersion == 1, "Unsupported OhmDB file format version: %s", fileVersion);

		dbstats.put(DbStats.FILE_VERSION, fileVersion);

		long oddTx1 = buf.getLong();
		long oddTx2 = buf.getLong();
		long oddTx3 = buf.getLong();

		long evenTx1 = buf.getLong();
		long evenTx2 = buf.getLong();
		long evenTx3 = buf.getLong();

		long latestTx = 0;
		if (oddTx1 == oddTx2 && oddTx2 == oddTx3 && oddTx1 > latestTx) {
			latestTx = oddTx1;
			firstTxCounter = false;
		}
		if (evenTx1 == evenTx2 && evenTx2 == evenTx3 && evenTx1 > latestTx) {
			latestTx = evenTx1;
			firstTxCounter = true;
		}

		Long[] commited = { oddTx1, oddTx2, oddTx3, evenTx1, evenTx2, evenTx3 };
		dbstats.put(DbStats.COMMITED, Arrays.asList(commited));

		dbstats.put(DbStats.LATEST_TRANSACTION, latestTx);
		txCounter.set(latestTx);

		// ...

		dbstats.put(DbStats.BLOCK_SIZE, BLOCK_SIZE);

		buf.position(HEADER_SIZE - MARK2.length());
		String mark2 = readStr(buf, MARK2.length());
		Check.state(mark2.equals(MARK2), "The file doesn't have a valid OhmDB format (incorrect mark2)!");
	}

	private String readStr(ByteBuffer buf, int length) {
		byte[] strBuf = new byte[length];
		buf.get(strBuf);
		return new String(strBuf);
	}

	@Override
	public synchronized void write(long key, Object value) throws IOException {
		// System.out.println("WRITE " + key + "=" + value);
		FilestoreTransaction tx = transaction();
		tx.write(key, value);

		final AtomicBoolean done = new AtomicBoolean();

		tx.addListener(new TransactionListener() {
			@Override
			public void onSuccess() {
				done.set(true);
			}

			@Override
			public void onError(Exception e) {
				e.printStackTrace(System.err);
				done.set(true);
			}
		});

		tx.commit();

		U.waitFor(done);
	}

	@Override
	public synchronized void delete(final long key) throws IOException {
		FilestoreTransaction tx = transaction();
		tx.delete(key);

		final AtomicBoolean done = new AtomicBoolean();

		tx.addListener(new TransactionListener() {
			@Override
			public void onSuccess() {
				done.set(true);
			}

			@Override
			public void onError(Exception e) {
				e.printStackTrace(System.err);
				done.set(true);
			}
		});

		tx.commit();

		U.waitFor(done);

	}

	private void write(Long key, Object value, long txId, boolean delete) throws IOException, BufferFullException {
		Check.arg(txId >= 0, "Transaction ID must be >= 0!");

		int start = BUF.position();

		try {
			// skip first header
			BUF.position(start + BLOCK_OVERHEAD);
		} catch (IllegalArgumentException e) {
			BUF.position(start);
			throw new BufferFullException();
		}

		try {
			writeKeyValueToBUF(key, value, delete);
			completeZerosInBUF(start);
		} catch (BufferOverflowException e) {
			BUF.position(start);
			throw new BufferFullException();
		}

		int bytesN = BUF.position() - start;

		int size = sizeOf(bytesN);

		if (aggregatedSize + size > MAX_BLOCKS) {
			BUF.position(start);
			throw new BufferFullException();
		}

		aggregatedSize += size;

		KeyAndSize kas = new KeyAndSize();

		kas.key = key;
		kas.size = size;
		kas.bytesN = bytesN;
		kas.delete = delete;

		aggregatedKeys.add(kas);
	}

	private void writeTx(long txId) throws IOException {
		Set<Long> slots = zones.occupy(aggregatedSize, aggregatedSize);
		assert slots.size() == aggregatedSize;

		BUF.position(0);

		Iterator<Long> it = slots.iterator();

		for (KeyAndSize ks : aggregatedKeys) {
			writeKS(it, ks.key, txId, ks.delete, ks.size, ks.bytesN);
		}

		assert !it.hasNext();
	}

	private void writeKS(Iterator<Long> it, Long key, long txId, boolean delete, int size, int bytesN)
			throws IOException {

		PersistInfo info = infos.getInfo(key);

		Set<Long> theSlots = new HashSet<Long>();
		long version = info.nextVersion(theSlots);

		// System.out.println("VERSION OF " + key + " IS " + version);

		long address = it.next();

		// System.out.println("writing " + key + " @ " + address);
		int start = BUF.position();

		long second = size > 1 ? it.next() : -1;
		putFirstInBUF(txId, delete, size, version, second);

		assert BUF.position() == start;

		assert bytesN >= BLOCK_SIZE;

		writeFirst(file, address);
		assert BUF.position() == start + BLOCK_SIZE;

		theSlots.add(address);

		if (size > 1) {

			long prev = address, addr = second, next;

			for (int p = 2; p <= size; p++) {

				// prev == next for the last part
				next = p < size ? it.next() : prev;

				writeNext(file, addr, prev, next);
				theSlots.add(addr);

				prev = addr;
				addr = next;
			}
		}

		int total = BUF.position() - start;
		assert total == bytesN;
		assert sizeOf(total) == size;

		// release previous data version
		VersionInfo ver = info.getVersion(version - 1);
		info.removeOldVersions();

		if (ver != null) {
			zones.releaseAll(ver.getSlots());
		}
	}

	private int sizeOf(int count) {
		if (count <= BLOCK_SIZE) {
			return 1;
		}

		int tailSize = count - BLOCK_SIZE;
		assert tailSize > 0;

		int tailPartSize = BLOCK_SIZE - 16;
		int tailN = tailSize / tailPartSize;

		if (tailSize % tailPartSize != 0) {
			tailN++; // for the remaining last part
		}

		return 1 + tailN;
	}

	private void writeKeyValueToBUF(Long key, Object value, boolean delete) {
		BUF.putLong(key);

		if (!delete) {
			valueCodec.encode(BUF, value); // simple types: 1-8B
		}
	}

	private void completeZerosInBUF(int start) {
		int length = BUF.position() - start;
		assert length > 0;

		if (length <= BLOCK_SIZE) {

			// complete zero's
			int zerosToPut = BLOCK_SIZE - length;

			putZeros(zerosToPut);

			// should be aligned now
			assert (BUF.position() - start) % BLOCK_SIZE == 0;

		} else {
			// complete zero's
			int tail = length - BLOCK_SIZE;
			int tailSize = BLOCK_SIZE - 16;

			int aloneBytes = tail % tailSize; // in the last block
			int zerosToPut = aloneBytes > 0 ? tailSize - aloneBytes : 0;

			putZeros(zerosToPut);

			int tail2 = BUF.position() - start - BLOCK_SIZE;
			assert tail2 % tailSize == 0;
		}

		assert BUF.position() - start - length < BLOCK_SIZE;
	}

	private void putZeros(int zerosToPut) {
		for (int i = 0; i < zerosToPut; i++) {
			BUF.put((byte) 0);
		}
	}

	private void putFirstInBUF(long txId, boolean delete, int size, long version, long next) throws IOException {
		int sizePosOrNeg = delete ? -size : size;
		int hash = hash(txId, sizePosOrNeg, version, next);

		int pos = BUF.position();

		BUF.putLong(txId); // transaction ID (8B)
		BUF.putLong(version); // 8B
		BUF.putInt(sizePosOrNeg); // 4B
		BUF.putLong(next); // next, 8B
		BUF.putInt(hash); // 4B

		BUF.position(pos);
	}

	private void writeFirst(RandomAccessFile fc, long address) throws IOException {
		assert address >= 0;

		// System.out.println("===== WRITING first block at @" + address +
		// hex(address));

		fc.seek(BASE_ADDRESS + address * BLOCK_SIZE);

		writeNBytes(fc, BUF, BLOCK_SIZE);
	}

	@SuppressWarnings("unused")
	private void writeFirstInformative(RandomAccessFile fc, long address) throws IOException {
		assert address >= 0;

		fc.seek(BASE_ADDRESS + address * BLOCK_SIZE);

		int pos = BUF.position();

		writeNBytes(fc, BUF, BLOCK_SIZE);

		long aa = BUF.getLong(pos);
		long bb = BUF.getLong(pos + 8);
		int cc = BUF.getInt(pos + 16);

		for (int i = 0; i < 32; i++) {
			byte by = BUF.get(pos + i + 32);
			char ch = by > 0 ? (char) by : '?';
		}
	}

	private void writeNext(RandomAccessFile fc, long address, long prev, long next) throws IOException {
		// System.out.println("next " + address + " " + prev + " " + next);

		assert address >= 0;
		assert debug("===== WRITING next block at @" + address);
		assert prev >= 0;
		assert next >= 0;

		fc.seek(BASE_ADDRESS + address * BLOCK_SIZE);

		assert BUF16.position() == 0;
		BUF16.putLong(negEncode(prev));
		BUF16.putLong(negEncode(next));
		assert BUF16.position() == 16;

		BUF16.flip();
		write(fc, BUF16, 16);
		BUF16.clear();

		writeNBytes(fc, BUF, BLOCK_SIZE - 16);
	}

	private void startWriting() throws IOException {
		long fileSize = file.length();
		if (fileSize == 0) {
			createDb();
		} else {
			Check.state(fileSize >= HEADER_SIZE, "The OhmDB file header is too small!");
		}
	}

	private int hash(long txId, int size, long version, long next) {
		int ver1 = (int) (version >> 32);
		int ver2 = (int) version;

		int tx1 = (int) (txId >> 32);
		int tx2 = (int) txId;

		int nxt1 = (int) (next >> 32);
		int nxt2 = (int) next;

		int hash = ((tx1 * 3) + 1) ^ (91 + tx2 * 5) ^ (23 - ver1 * 7) ^ (97 + ver2 * 11) ^ (3 + nxt1 * 13)
				^ (nxt2 * 17) ^ (size * 19);
		return hash;
	}

	private void write(RandomAccessFile fc, ByteBuffer out, int count) throws IOException {
		int real = 0;

		while (out.hasRemaining()) {
			real += fc.getChannel().write(out);
		}

		Check.state(count == real, "The buffer wasn't correctly writen to the file! Expected: %s, wrote: %s bytes",
				count, real);
	}

	private void writeNBytes(RandomAccessFile fc, ByteBuffer out, int count) throws IOException {
		int real = 0;

		out.limit(out.position() + count);

		while (out.hasRemaining()) {
			real += fc.getChannel().write(out);
		}

		Check.state(count == real, "The buffer wasn't correctly writen to the file! Expected: %s, wrote: %s bytes",
				count, real);

		out.limit(BUF.capacity());
	}

	private void transact(long txId, Map<Long, Object> values, Set<Long> deletedKeys) throws IOException,
			BufferFullException {

		for (Entry<Long, Object> entry : values.entrySet()) {
			// nice(" - WRITE #" + txId + " :: " + entry.getKey() + "=" +
			// entry.getValue());
			write(entry.getKey(), entry.getValue(), txId, false);
		}

		for (long delKey : deletedKeys) {
			// nice(" - DELETE #" + txId + " :: " + delKey);
			write(delKey, null, txId, true);
		}
		// nice("END #" + txId);
	}

	private void writeTxCounter(long txId) {
		// System.out.println(" - WRITE LATEST TX #" + txId);
		// nice(" - WRITE LATEST TX #" + txId);

		TX_BUF.clear();

		TX_BUF.putLong(txId);
		TX_BUF.putLong(txId);
		TX_BUF.putLong(txId);

		TX_BUF.flip();

		try {

			RandomAccessFile txf = new RandomAccessFile(filename, "rw");

			long pos = firstTxCounter ? 9 : 9 + 3 * 8;
			txf.seek(pos);
			write(txf, TX_BUF, 3 * 8);
			firstTxCounter = !firstTxCounter;

		} catch (IOException e) {
			throw Errors.rte(e);
		}
	}

	@Override
	public synchronized long getFileSize() {
		return new File(filename).length();
	}

	private void loadData(StoreLoader loader) {
		debug("====================================================================");

		assert (READ_BUF.capacity() % BLOCK_SIZE) == 0;

		try {
			RandomAccessFile fc = new RandomAccessFile(filename, "r");
			long fileSize = fc.length();

			loadHeader(fc);

			Check.state(fileSize < READ_BUF.capacity(), "Not enough read buffer!");

			FileChannel ch = fc.getChannel();
			while (ch.position() < fileSize) {
				long baseAddress = (ch.position() - BASE_ADDRESS) / BLOCK_SIZE;

				READ_BUF.clear();

				int readN = 0;
				int lastRead;
				do {
					lastRead = fc.getChannel().read(READ_BUF);
					if (lastRead > 0) {
						readN += lastRead;
					}
				} while (lastRead > 0 && READ_BUF.hasRemaining());

				assert readN % BLOCK_SIZE == 0;

				READ_BUF.flip();

				if (readN > 0) {
					READ_BUF.rewind(); // will read from the beginning

					loadBulk(loader, READ_BUF, baseAddress, readN);
				}
			}

			for (PersistInfo info : infos.entries()) {
				VersionInfo latest = info.getLatestVersion();
				// System.out.println(">> OCCUPIED " + latest.getSlots());
				zones.occupiedAll(latest.getSlots());
				info.removeOldVersions();
			}

		} catch (IOException e) {
			throw Errors.rte(e);
		}
		debug("===== Total blocks loaded: " + totalBlocks + " (" + errorBlocks + " of them corrupted)");
		debug("====================================================================");
		if (errorBlocks > 0) {
			throw Errors.rte("Total " + errorBlocks + " blocks were corrupted!");
		}
	}

	private void loadBulk(StoreLoader loader, ByteBuffer buf, long baseAddress, int readN) {
		assert buf.hasRemaining();
		assert debug("=== LOAD @" + buf.position() + " (base address=" + baseAddress + ")");

		int blocksN = readN / BLOCK_SIZE;

		int loadedN = 0;
		for (int i = 0; i < blocksN; i++) {
			boolean ok = loadBlock(loader, buf, baseAddress, i);
			if (ok) {
				loadedN++;
			}
		}

		debug("TOTAL LOADED BLOCKS = " + loadedN);
	}

	private boolean loadBlock(StoreLoader loader, ByteBuffer buf, long base, int offset) {
		// System.out.println("load block @" + base + " : " + offset);

		long address = base + offset;

		buf.position(offset * BLOCK_SIZE);

		long txOrder = buf.getLong(); // 8B
		long version = buf.getLong(); // 8B

		if (txOrder < 0) {
			return false;
		}

		totalBlocks++;

		int size = buf.getInt(); // 4B
		long next = buf.getLong(); // 8B;
		int hash = buf.getInt(); // 4B

		assert debug("tx=" + txOrder + " ver=" + version + " size=" + size + " CRC=" + hash);

		boolean success = true;

		int hashv = hash(txOrder, size, version, next);

		if (txOrder == 0 && size == 0 && hash == 0 && version == 0) {
			return false;
		}

		if (hash == hashv) {

			if (txOrder != TX_FREE_SPACE) {

				success &= check(txOrder >= 0, "Transaction order must be >= 0!");
				success &= check(version > 0, "Version must be greater than 0!");
				success &= check(size != 0, "Size must not be 0!");

				// load key and value if data was correct
				if (success) {
					try {
						loadFirst(loader, buf, base, offset, version, size, next);
					} catch (Throwable e) {
						throw Errors.rte("Cannot read data block!", e);
					}
				}
			} else {
				success &= check(version == 0, "Version must be 0!");
				success &= check(size != 0, "Size must not be 0!");
			}
		} else {
			success = false;
			errorBlocks++;
			error("Corrupted block detected at address " + address + hex(address) + " (" + "tx=" + txOrder + " ver="
					+ version + " size=" + size + " hash=" + hash + ")");
		}

		return success;
	}

	private String hex(long address) {
		return " [mem " + Long.toHexString(BASE_ADDRESS + address * BLOCK_SIZE) + "] ";
	}

	private int loadNext(final ByteBuffer buf, final Set<Long> slots, final long base, long commingFrom, long offset,
			int arrN) {

		boolean hasMore;

		do {
			long address = base + offset;
			slots.add(address);

			buf.position((int) offset * BLOCK_SIZE);

			long aa = negDecode(buf.getLong());
			long bb = negDecode(buf.getLong());
			hasMore = aa != bb;

			long prev = aa - base;

			if (!check(prev == commingFrom, "Broken block chain!")) {
				return 0;
			}

			buf.get(READ_ITEM_ARR, arrN, BLOCK_SIZE - 16);

			arrN += BLOCK_SIZE - 16;

			commingFrom = offset;
			offset = bb - base;

		} while (hasMore);

		return arrN;
	}

	private void loadFirst(StoreLoader loader, ByteBuffer buf, long base, int offset, long version, int size, long next) {

		long address = base + offset;

		boolean delete = size < 0;
		if (delete) {
			size = -size;
		}

		Set<Long> slots = U.set(address);

		buf.position(offset * BLOCK_SIZE + BLOCK_OVERHEAD);

		buf.get(READ_ITEM_ARR, 0, BLOCK_SIZE - BLOCK_OVERHEAD);

		int arrN = BLOCK_SIZE - BLOCK_OVERHEAD;

		if (next >= 0) {
			arrN = loadNext(buf, slots, base, offset, next - base, arrN);
		}

		READ_ITEM_BUF.position(0);
		READ_ITEM_BUF.limit(arrN);

		Long key;
		Object value = null;

		key = READ_ITEM_BUF.getLong();
		assert debug("DECODED Long: " + key);

		if (!delete) {
			value = valueCodec.decode(READ_ITEM_BUF);
		}

		PersistInfo info = infos.getInfo(key); // keeps key ref

		if (info.loadVersion(version, slots)) {
			if (delete) {
				nice("DELETE key=" + key);
				loader.delete(key);
			} else {
				nice("SET key=" + key + " : val=" + value);
				loader.set(key, value);
			}
		}
	}

	@SuppressWarnings("unused")
	private boolean skippedZeros(byte[] skipped) {
		for (int i = 0; i < skipped.length; i++) {
			if (skipped[i] != 0) {
				return false;
			}
		}
		return true;
	}

	private boolean check(boolean expectedCondition, String errorMsg) {
		if (!expectedCondition) {
			error(errorMsg);
		}
		return expectedCondition;
	}

	@Override
	public FilestoreTransaction transaction() {
		checkActive();
		return new FilestoreTransaction(this, db());
	}

	public Map<DbStats, Object> getStats() {
		return dbstats;
	}

	@Override
	public synchronized void clear() {
		throw Errors.notReady();
	}

	private boolean debug(String msg) {
		// System.out.println(msg);
		return true;
	}

	private void nice(String msg) {
		// System.out.println(msg);
	}

	private void error(String msg) {
		System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		System.err.println("!!! OhmDB ERROR: " + msg);
		System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
	}

	private long negEncode(long n) {
		assert n >= 0;
		return Long.MIN_VALUE + n;
	}

	private long negDecode(long n) {
		assert n < 0;
		return n - Long.MIN_VALUE;
	}

	@Override
	public void commit(FilestoreTransaction tx) {
		while (!txs.offer(tx)) {
			U.sleep(5);
		}
	}

	@Override
	public void rollback(FilestoreTransaction tx) {
		releaseTx(tx);
	}

	private boolean transact(long txId, FilestoreTransaction tx) throws BufferFullException {
		try {
			transact(txId, tx.values, tx.deleted);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		} finally {
			// tx.off("transact");
		}
	}

	@Override
	public void run() {
		// executes in separate thread in parallel
		try {

			FilestoreTransaction tx;

			List<FilestoreTransaction> currentTxs = new ArrayList<FilestoreTransaction>(20000);

			while ((dbExists() && running.get()) || !txs.isEmpty()) {
				boolean transacted = true;

				while (txs.isEmpty() && running.get()) {
					U.sleep(10);
				}

				BUF.rewind();
				long txId = txCounter.incrementAndGet();
				int n = 0;
				aggregatedSize = 0;
				aggregatedKeys.clear();
				currentTxs.clear();

				while (transacted && (tx = txs.peek()) != null) {

					try {
						transacted = tx.isReadOnly() ? true : transact(txId, tx);
					} catch (BufferFullException e) {
//						System.out.println("*** BUFFER FULL! ***");
						transacted = false;
						if (currentTxs.isEmpty()) {
							throw Errors.rte("The transaction is too big!");
						}
					}

					if (transacted) {
						n++;
						DatastoreTransaction tx2 = txs.poll();
						assert tx == tx2;
						currentTxs.add(tx);
					}
				}

				debug("TRANSACTING N=" + n);

				// System.out.println("====================================");
				// if there is at least one change in transactions
				if (aggregatedSize > 0) {
					writeTx(txId);
					writeTxCounter(txId);
					file.getChannel().force(false);
				}

				for (FilestoreTransaction txx : currentTxs) {
					txx.success();
					releaseTx(txx);
				}
			}

		} catch (Throwable e) {
			if (running.get()) {
				Db db = dbRef.get();
				if (db != null) {
					((OhmDBImpl) db).failure(e);
				}
				throw Errors.rte(e);
			} else {
				e.printStackTrace();
			}
		}

		System.out.println("File store thread finished.");
		finished.set(true);
	}

	private boolean dbExists() {
		return dbRef == null || dbRef.get() != null;
	}

	@Override
	public void stop() {
		super.stop();
		thread.interrupt();
	}

	@Override
	public void shutdown() {
		super.shutdown();

		while (!finished.get()) {
			U.sleep(10);
		}

		try {
			file.close();
		} catch (IOException e) {
			throw Errors.rte("Couldn't close database file!", e);
		}
	}

	private void releaseTx(FilestoreTransaction tx) {
		tx.done();
	}

}
