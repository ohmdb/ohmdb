package com.ohmdb.relation;

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

import org.testng.annotations.Test;

import com.ohmdb.api.Db;
import com.ohmdb.api.ManyToMany;
import com.ohmdb.api.Ohm;
import com.ohmdb.test.TestCommons;
import com.ohmdb.util.U;

public class RelLinkTest extends TestCommons {

	private static final String DB_FILENAME = "/tmp/benchmark.db";

	@Test
	public void testMassiveLinks() {
		for (int k = 0; k < 100; k++) {

			int a = 100;
			int b = 50;

			U.delete(DB_FILENAME);

			Db db = Ohm.db(DB_FILENAME);

			ManyToMany<Object, Object> fr = db.manyToMany(null, "friends", null);

			long op = 100000000;

			for (int i = 0; i < a; i++) {
				for (int j = 0; j < b; j++) {
					fr.link(i, op++);
				}
			}

			db.shutdown();

			Db db2 = Ohm.db(DB_FILENAME);
			db2.shutdown();
		}
	}

}
