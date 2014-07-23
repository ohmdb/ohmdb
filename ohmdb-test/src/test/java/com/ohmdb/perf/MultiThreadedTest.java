package com.ohmdb.perf;

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
import com.ohmdb.test.TestCommons;

public class MultiThreadedTest extends TestCommons {

	private static final int total = 1000;

	@Test
	public void shouldPerformWell() throws Exception {

		Thread[] threads = new Thread[100];
		for (int i = 0; i < threads.length; i++) {

			threads[i] = new Thread() {
				public void run() {
					Table<Person> persons = db.table(Person.class);
					long[] ids = new long[total];

					for (int x = 0; x < total; x++) {
						ids[x] = persons.insert(person("p" + x, x % 2));
					}

					// System.out.println(persons.size());

					for (int x = 0; x < total; x++) {
						Person person = persons.get(ids[x]);
						eq(person.name, "p" + x);
						eq(person.age, x % 2);
					}

					for (int x = 0; x < total; x++) {
						Object val = persons.read(ids[x], "name");
						eq(val, "p" + x);
					}

					for (int x = 0; x < total; x++) {
						Object val = persons.read(ids[x], "age");
						eq(val, x % 2);
					}

					for (int x = 0; x < total; x++) {
						persons.delete(ids[x]);
					}

				};
			};
		}

		for (int i = 0; i < threads.length; i++) {
			threads[i].start();
		}

		for (int i = 0; i < threads.length; i++) {
			threads[i].join();
		}

	}

}
