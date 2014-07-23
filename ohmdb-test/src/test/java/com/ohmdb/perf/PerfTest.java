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

import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

import com.ohmdb.api.Table;
import com.ohmdb.test.Person;
import com.ohmdb.test.TestCommons;
import com.ohmdb.util.Measure;
import com.ohmdb.util.U;

public class PerfTest extends TestCommons {

	private static final int total = 10 * 1000;

	@Test
	public void shouldPerformWell() throws Exception {

		long[] ref = new long[total];

		AtomicInteger n;

		Table<Person> persons = personsTable();

		for (int times = 0; times < 1; times++) {
			persons.clear();

			Measure.start(total);

			n = new AtomicInteger();

			for (int x = 0; x < total; x++) {
				tx(n);

				ref[x] = persons.insert(person("fff" + x, x % 2));

				commit();
			}

			U.waitFor(n, total);

			Measure.finish("insert");

			/****************************************************************/

			Measure.start(total);

			n = new AtomicInteger();
			for (int x = 0; x < total; x++) {
				tx(n);

				Person person = persons.get(ref[x]);
				eq(person.name, "fff" + x);
				eq(person.age, x % 2);

				commit();
			}
			U.waitFor(n, total);

			Measure.finish("get");

			Measure.start(total);

			n = new AtomicInteger();
			for (int x = 0; x < total; x++) {
				tx(n);

				Object val = persons.read(ref[x], "name");
				eq(val, "fff" + x);

				commit();
			}
			U.waitFor(n, total);

			Measure.finish("read-string");

			Measure.start(total);

			n = new AtomicInteger();
			for (int x = 0; x < total; x++) {
				tx(n);

				Object val = persons.read(ref[x], "age");
				eq(val, x % 2);

				commit();
			}
			U.waitFor(n, total);

			Measure.finish("read-int");

			Measure.start(total);

			n = new AtomicInteger();
			for (int x = 0; x < total; x++) {
				tx(n);

				persons.update(ref[x], person("ggg" + x, x % 2));

				commit();
			}
			U.waitFor(n, total);

			Measure.finish("update");

			n = new AtomicInteger();
			for (int x = 0; x < total; x++) {
				tx(n);

				persons.delete(ref[x]);

				commit();
			}
			U.waitFor(n, total);

			Measure.finish("delete");

			Measure.start(total);

			n = new AtomicInteger();
			for (int x = 0; x < total; x++) {
				tx(n);

				persons.insert(person("ggg" + x, x % 2));

				commit();
			}
			U.waitFor(n, total);

			Measure.finish("refill");

		}

		Measure.stats();
	}
}
