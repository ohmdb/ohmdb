package com.ohmdb.transaction;

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

import com.ohmdb.api.Table;
import com.ohmdb.test.Person;
import com.ohmdb.test.SimpleParallel;
import com.ohmdb.test.TestCommons;
import com.ohmdb.test.ThreadPack;
import com.ohmdb.util.Measure;
import com.ohmdb.util.U;

public class TransactionIsolationTest extends TestCommons {

	@Test
	public void shouldIsolateTransactions() {
		final Table<Person> persons = personsTable();

		persons.insert(person("john", 29));

		int threads = 100;
		int iterations = 1000;

		/*** INSERT IN PARALLEL TRANSACTIONS ***/

		ThreadPack readThreads = threads("read", 1, 100000000, false, new SimpleParallel() {

			@Override
			public void run(int threadN, int cycleN) {
				// persons.size();
				U.sleep(11);
			}

		});

		ThreadPack writeThreads = threads("write", threads, iterations, false, new SimpleParallel() {

			@Override
			public void run(int threadN, int cycleN) {
//				long id0, id1;

				tx();
				persons.set(0, "age", 234);
				commit();

				// int startingSize = persons.size();
				// isTrue(startingSize % 2 == 0);

				// try {

				// id0 = persons.insert(person("john", 29));
				//
				// eq(persons.size(), startingSize + 1);
				//
				// id1 = persons.insert(person("bill", 13));
				//
				// eq(persons.size(), startingSize + 2);
				//
				// tx.commit();
				//
				// } finally {
				// tx.close();
				// }

				// int endingSize = persons.size();
				// isTrue(endingSize % 2 == 0);
				// isTrue(endingSize >= startingSize + 2);
				//
				// Person p0 = persons.get(id0);
				// eq(p0.name, "john");
				// eq(p0.age, 29);
				//
				// Person p1 = persons.get(id1);
				// eq(p1.name, "bill");
				// eq(p1.age, 13);
			}

		});

		Measure.start(threads * iterations);

		writeThreads.start();
		readThreads.start();

		ThreadPack.finishFirst(db, writeThreads, readThreads);

		Measure.finish("writes");

		// db.stats();

		System.out.println("Table size: " + persons.size());
	}

}
