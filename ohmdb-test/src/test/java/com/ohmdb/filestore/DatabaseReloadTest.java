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

import org.testng.annotations.Test;

import com.ohmdb.api.Ohm;
import com.ohmdb.test.Book;
import com.ohmdb.test.Parallel;
import com.ohmdb.test.Person;
import com.ohmdb.test.TestCommons;
import com.ohmdb.test.ThreadPack;

public class DatabaseReloadTest extends TestCommons {

	@Test(dataProvider = "num100")
	public void singleSmallInsertions(int len) {
		persons = db.table(Person.class);

		db.table(Person.class).insert(person(rndStr(len), 2));

		eq(db.table(Person.class).size(), 1);

		reload();

		eq(db.table(Person.class).size(), 1);
	}

	@Test(dataProvider = "num100")
	public void singleBigInsertions(int len) {
		persons = db.table(Person.class);

		db.table(Person.class).insert(person(rndStr(len * 1000), 2));

		eq(db.table(Person.class).size(), 1);

		reload();

		eq(db.table(Person.class).size(), 1);
	}

	@Test
	public void shoudReloadDB() {
		initSchema();

		for (int i = 0; i < 10000; i++) {
			db.table(Person.class).insert(person("aa" + i, 2));
		}

		eq(db.table(Person.class).size(), 10000);

		reload();

		eq(db.table(Person.class).size(), 10000);

		for (int i = 0; i < 10000; i++) {
			db.table(Person.class).insert(person("bb" + i, 2));
		}

		eq(db.table(Person.class).size(), 20000);

		reload();

		eq(db.table(Person.class).size(), 20000);

		for (int i = 0; i < 10000; i++) {
			db.table(Person.class).insert(person("bb" + i, 2));
		}

		eq(db.table(Person.class).size(), 30000);

		reload();

		eq(db.table(Person.class).size(), 30000);
	}

	@Test
	public void shoudReloadDBWithMultiThreadedWrites() {
		initSchema();

		insertN();

		eq(db.table(Person.class).size(), 10000);

		db.shutdown();

		db = Ohm.db(DB_FILE);

		insertN();

		eq(db.table(Person.class).size(), 20000);

		db.shutdown();

		db = Ohm.db(DB_FILE);

		insertN();

		eq(db.table(Person.class).size(), 30000);

		db.shutdown();

		db = Ohm.db(DB_FILE);

		insertN();

		eq(db.table(Person.class).size(), 40000);

		db.shutdown();
	}

	private void insertN() {
		ThreadPack pack = threads("f", 1, 10000, false, new Parallel() {
			@Override
			public void run(int threadN, int cycleN) {
				db.table(Person.class).insert(person("aa" + threadN, cycleN));
				db.table(Book.class).insert(book("bb" + threadN, true));
			}

			@Override
			public void init(int threadN) {
			}
		}).start();

		ThreadPack.finish(pack);
	}

}
