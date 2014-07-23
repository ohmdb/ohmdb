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

public class TransactionRollbackTest extends TestCommons {

	@Test
	public void shouldRollbackTransactions() {
		final Table<Person> persons = personsTable();

		int threads = 1000;
		int iterations = 1000;

		/*** INSERT IN PARALLEL TRANSACTIONS ***/

		final int startingSize = persons.size();

		ThreadPack pack = threads("write", threads, iterations, false, new SimpleParallel() {

			@Override
			public void run(int threadN, int cycleN) {
				tx();

				eq(persons.size(), startingSize);

				persons.insert(person("john", 29));

				eq(persons.size(), startingSize + 1);

				persons.insert(person("bill", 13));

				eq(persons.size(), startingSize + 2);

				rollback();

				eq(persons.size(), startingSize);

				// System.out.println(id0 + ":" + id1);
			}

		});

		Measure.start(2 * threads * iterations);

		pack.start();

		ThreadPack.finish(pack);

		Measure.finish("writes + rollbacks");

		System.out.println("Table size: " + persons.size());
	}

}
