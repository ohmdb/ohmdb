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

import java.util.Date;
import java.util.Random;

import com.ohmdb.abstracts.RWRelation;
import com.ohmdb.api.Table;
import com.ohmdb.numbers.Numbers;
import com.ohmdb.test.Person;
import com.ohmdb.test.TestCommons;
import com.ohmdb.util.UTILS;

public class RelationPerfTest extends TestCommons {

	private static final int total = 1000000;

	public static void main(String[] args) throws Exception {
		new RelationPerfTest().run();
	}

	private void run() {
		System.out.println(new Date());

		Table<Person> t1 = personsTable();
		Table<Person> t2 = personsTable();

		RWRelation rel = relation(db, t1, "wrote", t2);

		Random rnd = new Random();

		while (true) {
			rel.clear();

			time("start");

			for (long i = 0; i < total; i++) {
				Numbers fff = nums(rnd.nextInt(total), rnd.nextInt(total), rnd.nextInt(total));
				UTILS.link(rel, i, fff);
			}

			time("link");

			for (long i = 0; i < total; i++) {
				rel.linksFrom(i);
			}

			time("find links from");

			for (long i = 0; i < total; i++) {
				rel.linksTo(i);
			}

			time("find links to");
		}

	}

}
