package com.ohmdb.statistical;

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

import com.ohmdb.test.DatabaseCheck;
import com.ohmdb.test.Person;
import com.ohmdb.test.TableShadow;
import com.ohmdb.test.TestCommons;

public class SimpleTableStatisticalTest extends TestCommons {

	protected static final int MINI_REL_COUNT = 20;

	@Test(dataProvider = "num10")
	public void shouldKeepRelationsBetweenTables(int factor) {

		final int total = 1000;

		final int threadsFactor = factor + 1; // 1, 2, 3...
		final int refreshMs = 20 * factor + 5; // 5, 25, 55...

		final DatabaseCheck db = new DatabaseCheck(refreshMs, threadsFactor);

		final AtomicInteger N = new AtomicInteger();

		db.register(new TableChecker<Person>("insert person", Person.class) {
			@Override
			public void check(TableShadow<Person> table, int iteration) {
				table.insert(new Person("john", 29));
				N.incrementAndGet();
			}
		});

		db.register(new TableChecker<Person>("delete person", Person.class) {
			@Override
			public void check(TableShadow<Person> table, int iteration) {
				table.delete(rnd(N.get() + 10));
			}
		});

		db.execute(total);
	}
}
