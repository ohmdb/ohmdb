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

import java.util.Map;

import org.testng.annotations.Test;

import com.ohmdb.abstracts.DatastoreTransaction;
import com.ohmdb.codec.StrCodec;
import com.ohmdb.impl.OhmDBStats;
import com.ohmdb.test.TestCommons;

public class FileStoreTransactionsOrderTest extends TestCommons {

	private static final StrCodec STR_CODEC = new StrCodec();

	@Test
	public void shoudSupportTransactionsInAnyOrder() {
		OhmDBStats stats = new OhmDBStats();

		Map<Long, Object> check = storeMap();

		MapBackedStoreLoader loader = new MapBackedStoreLoader();
		FileStore store = new FileStore(DB_FILE, loader, STR_CODEC, stats, false, DB_REF);

		DatastoreTransaction tx1 = store.transaction();
		DatastoreTransaction tx2 = store.transaction();
		DatastoreTransaction tx3 = store.transaction();

		tx1.write(1, "a");
		check.put(1L, "a");

		tx1.write(2, "b");
		check.put(2L, "b");

		tx2.write(3, "c");
		check.put(3L, "c");

		tx2.write(4, "d");
		check.put(4L, "d");

		tx3.write(5, "e");
		check.put(5L, "e");

		tx3.write(6, "f");
		check.put(6L, "f");

		tx3.commit();
		tx2.commit();
		tx1.commit();

		waitTx(tx1, tx2, tx3);
		store.stop();

		MapBackedStoreLoader loader2 = new MapBackedStoreLoader();
		FileStore store2 = new FileStore(DB_FILE, loader2, STR_CODEC, stats, true, DB_REF);

		System.out.println(store2.getStats());
		Map<Long, Object> values = loader2.getValues();
		eq(values, check);
	}

}
