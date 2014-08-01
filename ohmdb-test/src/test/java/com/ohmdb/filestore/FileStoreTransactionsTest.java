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
import java.util.Arrays;
import java.util.Map;

import org.testng.annotations.Test;

import com.ohmdb.abstracts.DatastoreTransaction;
import com.ohmdb.codec.StrCodec;
import com.ohmdb.impl.OhmDBStats;
import com.ohmdb.test.TestCommons;

public class FileStoreTransactionsTest extends TestCommons {

	private static final StrCodec STR_CODEC = new StrCodec();

	// TODO: multiple transactions
	// LIMITATION: serial transaction commits, ordered by txID

	@Test
	public void shoudSupportTransactions() {
		new File(DB_FILE).delete();
		OhmDBStats stats = new OhmDBStats();

		Map<Long, Object> check = storeMap();

		MapBackedStoreLoader loader = new MapBackedStoreLoader();
		FileStore store = new FileStore(DB_FILE, loader, STR_CODEC, stats, false, DB_REF);

		Map<DbStats, Object> statss1 = store.getStats();
		System.out.println(statss1);
		eq(statss1.get(DbStats.COMMITED), null);

		DatastoreTransaction tx1 = store.transaction();

		tx1.write(1, "a");
		check.put(1L, "a");

		tx1.write(2, "b");
		check.put(2L, "b");

		tx1.commit();

		waitTx(tx1);
		store.shutdown();

		MapBackedStoreLoader loader2 = new MapBackedStoreLoader();
		FileStore store2 = new FileStore(DB_FILE, loader2, STR_CODEC, stats, false, DB_REF);

		Map<Long, Object> values = loader2.getValues();
		eq(values, check);

		Map<DbStats, Object> statss = store2.getStats();
		System.out.println(statss);
		Long[] commited = { 1L, 1L, 1L, 0L, 0L, 0L };
		eq(statss.get(DbStats.COMMITED), Arrays.asList(commited));

		DatastoreTransaction tx2 = store2.transaction();

		tx2.write(10, "aa");
		check.put(10L, "aa");

		tx2.write(20, "bb");
		check.put(20L, "bb");

		tx2.commit();

		waitTx(tx2);
		store2.shutdown();

		MapBackedStoreLoader loader3 = new MapBackedStoreLoader();
		FileStore store3 = new FileStore(DB_FILE, loader3, STR_CODEC, stats, true, DB_REF);

		values = loader3.getValues();
		eq(values, check);

		statss = store3.getStats();
		System.out.println(statss);
		Long[] commited2 = { 1L, 1L, 1L, 2L, 2L, 2L };
		eq(statss.get(DbStats.COMMITED), Arrays.asList(commited2));
	}

}
