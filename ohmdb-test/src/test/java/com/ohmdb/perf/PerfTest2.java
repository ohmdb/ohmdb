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

import com.ohmdb.api.Ohm;
import com.ohmdb.api.OhmDB;
import com.ohmdb.api.Table;
import com.ohmdb.api.Transaction;
import com.ohmdb.test.Person;
import com.ohmdb.test.TestCommons;
import com.ohmdb.util.Measure;
import com.ohmdb.util.U;

public class PerfTest2 extends TestCommons {

	private static final int total = 300 * 1000;

	@Test
	public void shouldInsertFastInMemInTx() throws Exception {

		OhmDB db2 = Ohm.db();

		Table<Person> persons = db2.table(Person.class);

		Measure.start(total);

		AtomicInteger n = atomN();

		Person person = person("fff", 22);
		for (int x = 0; x < total; x++) {
			Transaction ttx = db2.startTransaction();
			ttx.addListener(txInc(n));

			persons.insert(person);

			ttx.commit();
		}

		U.waitFor(n, total);

		Measure.finish("insert");

		Measure.stats();
	}

	@Test
	public void shouldInsertFastInMemNoTx() throws Exception {

		OhmDB db2 = Ohm.db();

		Table<Person> persons = db2.table(Person.class);

		Measure.start(total);

		Person person = person("fff", 22);
		for (int x = 0; x < total; x++) {
			persons.insert(person);
		}

		Measure.finish("insert");

		Measure.stats();
	}

}
