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
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

import com.ohmdb.codec.IntCodec;
import com.ohmdb.impl.OhmDBStats;
import com.ohmdb.test.TestCommons;
import com.ohmdb.util.Measure;
import com.ohmdb.util.U;

public class FileStoreTransactPerfTest extends TestCommons {

	private static final IntCodec INT_CODEC = new IntCodec();

	@Test(timeOut = 30000)
	public void shoudPerformWell() throws IOException {
		new File(DB_FILE).delete();
		OhmDBStats stats = new OhmDBStats();

		FileStore store = new FileStore(DB_FILE, null, INT_CODEC, stats, false, DB_REF);

		int count = 300 * 1000;

		Measure.start(count);

		AtomicInteger n = new AtomicInteger();

		for (int i = 0; i < count; i++) {
			FilestoreTransaction tr = store.transaction();
			tr.write(i, i * 10);
			tr.addListener(this.txInc(n));
			tr.commit();
		}

		U.waitFor(n, count);

		Measure.finish("transactions");
		store.stop();

		DataLoader loader = new DataLoader();
		new FileStore(DB_FILE, loader, INT_CODEC, stats, false, null);
		eq(loader.getData().count(), count);
	}

}
