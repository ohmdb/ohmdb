package com.ohmdb.filestore;

/*
 * #%L
 * ohmdb-test
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
import java.util.Map;
import java.util.Random;

import org.testng.annotations.Test;

import com.ohmdb.codec.IntCodec;
import com.ohmdb.codec.StrCodec;
import com.ohmdb.impl.OhmDBStats;
import com.ohmdb.test.TestCommons;
import com.ohmdb.util.Measure;

public class FileStoreTest extends TestCommons {

	private static final String FILE = "/tmp/store";

	private static final IntCodec INT_CODEC = new IntCodec();

	private static final StrCodec STR_CODEC = new StrCodec();

	@Test
	public void shoudSimplyWork() throws IOException {
		new File(FILE).delete();
		OhmDBStats stats = new OhmDBStats();

		Map<Long, Object> check = storeMap();

		MapBackedStoreLoader loader = new MapBackedStoreLoader();
		FileStore store = new FileStore(FILE, loader, STR_CODEC, stats, false, DB_REF);

		store.write(123, "abc");
		check.put(123L, "abc");

		store.stop();

		MapBackedStoreLoader loader2 = new MapBackedStoreLoader();
		new FileStore(FILE, loader2, STR_CODEC, stats, false, DB_REF);
		Map<Long, Object> values = loader2.getValues();

		eq(values, check);
	}

	@Test
	public void shoudReuseOldSpace() throws IOException {
		new File(FILE).delete();
		OhmDBStats stats = new OhmDBStats();

		FileStore store = new FileStore(FILE, null, INT_CODEC, stats, false, DB_REF);

		int count = 100;

		for (int i = 0; i < count; i++) {
			store.write(i, i * 10);
		}

		eq(store.getFileSize(), (long) count * FileStore.BLOCK_SIZE + FileStore.HEADER_SIZE);

		for (int i = 0; i < count; i++) {
			store.write(i, i * 20);
		}

		eq(store.getFileSize(), (long) (count + 1) * FileStore.BLOCK_SIZE + FileStore.HEADER_SIZE);

		store.stop();
	}

	@Test
	public void shoudSupportAnyValue() throws IOException {
		new File(FILE).delete();
		Random rnd = new Random();
		OhmDBStats stats = new OhmDBStats();

		Map<Long, Object> check = storeMap();

		for (int n = 0; n < 50; n++) {
			MapBackedStoreLoader loader = new MapBackedStoreLoader();
			FileStore store = new FileStore(FILE, loader, INT_CODEC, stats, false, DB_REF);

			for (int i = 0; i < rnd.nextInt(10); i++) {
				long key = rnd.nextInt(1000);
				int value = rnd.nextInt();
				store.write(key, value);
				check.put(key, value);
			}

			store.stop();
		}

		MapBackedStoreLoader loader = new MapBackedStoreLoader();
		new FileStore(FILE, loader, INT_CODEC, stats, true, DB_REF);
		Map<Long, Object> values = loader.getValues();

		eq(values, check);
	}

	@Test
	public void shoudPersistChanges() throws IOException {
		new File(FILE).delete();
		OhmDBStats stats = new OhmDBStats();

		Map<Long, Object> check = storeMap();

		for (int n = 0; n < 200; n++) {
			MapBackedStoreLoader loader = new MapBackedStoreLoader();
			FileStore store = new FileStore(FILE, loader, INT_CODEC, stats, false, DB_REF);

			for (int i = 1; i <= 10; i++) {
				long key = i * 10;
				int value = i * 100;
				store.write(key, value);
				check.put(key, value);
			}

			store.stop();
		}

		MapBackedStoreLoader loader = new MapBackedStoreLoader();
		new FileStore(FILE, loader, INT_CODEC, stats, true, DB_REF);
		Map<Long, Object> values = loader.getValues();

		eq(values, check);
	}

	@Test(timeOut = 5000)
	public void shoudPerformWell() throws IOException {
		new File(FILE).delete();
		OhmDBStats stats = new OhmDBStats();

		FileStore store = new FileStore(FILE, null, INT_CODEC, stats, false, DB_REF);

		int count = 10;
		int iterations = 10;

		Measure.start(iterations * count);

		for (int j = 0; j < iterations; j++) {

			for (int i = 0; i < count; i++) {
				store.write(i, i * 10 + j);
			}

			if (j == 0) {
				eq(store.getFileSize(), (long) count * FileStore.BLOCK_SIZE + FileStore.HEADER_SIZE);
			} else {
				eq(store.getFileSize(), (long) (count + 1) * FileStore.BLOCK_SIZE + FileStore.HEADER_SIZE);
			}
		}

		Measure.finish("write");

		store.stop();
	}

	@Test
	public void shoudSupportBigValuesAtTheBeginning() throws IOException {
		new File(FILE).delete();
		OhmDBStats stats = new OhmDBStats();

		String big = big();

		Map<Long, Object> check = storeMap();

		MapBackedStoreLoader loader = new MapBackedStoreLoader();
		FileStore store = new FileStore(FILE, loader, STR_CODEC, stats, false, DB_REF);

		store.write(1, big);
		check.put(1L, big);

		store.write(2, "a");
		check.put(2L, "a");

		store.write(3, "b");
		check.put(3L, "b");

		store.stop();

		MapBackedStoreLoader loader2 = new MapBackedStoreLoader();
		new FileStore(FILE, loader2, STR_CODEC, stats, true, DB_REF);
		Map<Long, Object> values = loader2.getValues();

		eq(values, check);
	}

	@Test
	public void shoudSupportBigValuesInTheMiddle() throws IOException {
		new File(FILE).delete();
		OhmDBStats stats = new OhmDBStats();

		String big = big();

		Map<Long, Object> check = storeMap();

		MapBackedStoreLoader loader = new MapBackedStoreLoader();
		FileStore store = new FileStore(FILE, loader, STR_CODEC, stats, false, DB_REF);

		store.write(1, "a");
		check.put(1L, "a");

		store.write(2, big);
		check.put(2L, big);

		store.write(3, "c");
		check.put(3L, "c");

		store.stop();

		MapBackedStoreLoader loader2 = new MapBackedStoreLoader();
		new FileStore(FILE, loader2, STR_CODEC, stats, true, DB_REF);
		Map<Long, Object> values = loader2.getValues();

		eq(values, check);
	}

	@Test
	public void shoudSupportBigValuesAtTheEnd() throws IOException {
		new File(FILE).delete();
		OhmDBStats stats = new OhmDBStats();

		String big = big();

		Map<Long, Object> check = storeMap();

		MapBackedStoreLoader loader = new MapBackedStoreLoader();
		FileStore store = new FileStore(FILE, loader, STR_CODEC, stats, false, DB_REF);

		store.write(1, "a");
		check.put(1L, "a");

		store.write(2, "b");
		check.put(2L, "b");

		store.write(3, big);
		check.put(3L, big);

		store.stop();

		MapBackedStoreLoader loader2 = new MapBackedStoreLoader();
		new FileStore(FILE, loader2, STR_CODEC, stats, true, DB_REF);
		Map<Long, Object> values = loader2.getValues();

		eq(values, check);
	}

	private String big() {
		// subtract 4B key + 4 B str length
		int size = FileStore.BLOCK_SIZE * 100;

		StringBuilder sb = new StringBuilder(size);
		for (int i = 0; i < size; i++) {
			sb.append('x');
		}

		String big = sb.toString();
		return big;
	}

}
