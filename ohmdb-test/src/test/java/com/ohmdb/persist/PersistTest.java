package com.ohmdb.persist;

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

import java.io.IOException;

import org.testng.annotations.Test;

import com.ohmdb.api.Ohm;
import com.ohmdb.api.Db;
import com.ohmdb.api.Table;
import com.ohmdb.api.Transaction;
import com.ohmdb.api.TransactionException;
import com.ohmdb.test.Person;
import com.ohmdb.test.TestCommons;
import com.ohmdb.util.U;

public class PersistTest extends TestCommons {

	@Test
	public void shouldPersistToFile() throws IOException {

		/*** WRITE DB 1 ***/

		Db db = Ohm.db(DB_FILE2);
		Table<Person> table = db.table(Person.class);

		assert 0 == table.insert(person("a", 100));
		assert 1 == table.insert(person("b", 200));

		for (int i = 0; i < 3; i++) {
			table.set(0, "name", "x" + i);
			table.set(0, "age", i);
		}

		table.print();

		db.shutdown();

		/*** READ DB 2 ***/

		Db db2 = Ohm.db(DB_FILE2);
		Table<Person> table2 = db2.table(Person.class);

		eq(table2.size(), 2);
		table2.print();

		/*** WRITE DB 2 ***/

		long id2 = table2.insert(person("c", 300));
		eq(id2, 2L);

		eq(table2.size(), 3);

		table2.set(0, "name", "x");
		table2.set(1, "name", "y");

		table2.delete(1);
		eq(table2.size(), 2);

		table2.set(2, "name", "z");

		table2.print();

		db2.shutdown();

		/*** READ DB 3 ***/

		Db db3 = Ohm.db(DB_FILE2);
		Table<Person> table3 = db3.table(Person.class);

		eq(table3.size(), 2);
		table3.print();

		db3.shutdown();
	}

	@Test
	public void shouldPersistToFileMini() throws IOException, TransactionException {
		Db db = Ohm.db(DB_FILE2);
		Table<Person> table = db.table(Person.class);

		assert 0 == table.insert(person("a", 100));
		assert 1 == table.insert(person("b", 200));

		db.shutdown();

		/*** READ DB 2 ***/

		Db db2 = Ohm.db(DB_FILE2);
		Table<Person> table2 = db2.table(Person.class);

		Transaction txx = db2.startTransaction();

		eq(table2.size(), 2);

		System.out.println("now delete...");
		table2.delete(1);

		eq(table2.size(), 1);

		txx.commit();

		Transaction txx2 = db2.startTransaction();

		System.out.println("now set name");
		table2.set(0, "name", "z");

		txx2.commit();

		db2.shutdown();

		/*** READ DB 3 ***/

		U.sleep(1000);
		System.out.println("=============================");

		Db db3 = Ohm.db(DB_FILE2);

		Table<Person> table3 = db3.table(Person.class);
		eq(table3.size(), 1);
	}

}
