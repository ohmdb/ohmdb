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

import com.ohmdb.api.ManyToMany;
import com.ohmdb.api.OneToOne;
import com.ohmdb.test.Person;
import com.ohmdb.test.TestCommons;

public class SymetricRelationTest extends TestCommons {

	@Test
	public void shouldKeepOneToOneSymetric1() {
		initData10();
		OneToOne<Person, Person> rel = db.oneToOneSymmetric(persons, "rel", persons);

		rel.link(1, 2);
		rel.link(3, 4);

		eq(rel.from(1), 2);
		eq(rel.from(2), 1);
		eq(rel.from(3), 4);
		eq(rel.from(4), 3);

		eq(rel.to(1), 2);
		eq(rel.to(2), 1);
		eq(rel.to(3), 4);
		eq(rel.to(4), 3);

		hasId(rel.from(p(1)), 2);
		hasId(rel.from(p(2)), 1);
		hasId(rel.from(p(3)), 4);
		hasId(rel.from(p(4)), 3);

		hasId(rel.to(p(1)), 2);
		hasId(rel.to(p(2)), 1);
		hasId(rel.to(p(3)), 4);
		hasId(rel.to(p(4)), 3);

		eq(rel.to(100), -1);
		eq(rel.from(100), -1);

		eq(rel.to(p(100)), null);
		eq(rel.from(p(100)), null);

		rel.delink(4, 3);

		eq(rel.to(3), -1);
		eq(rel.to(4), -1);
		eq(rel.from(3), -1);
		eq(rel.from(4), -1);

		eq(rel.to(p(3)), null);
		eq(rel.to(p(4)), null);
		eq(rel.from(p(3)), null);
		eq(rel.from(p(4)), null);

		eq(rel.from(1), 2);
		eq(rel.from(2), 1);
		eq(rel.to(1), 2);
		eq(rel.to(2), 1);
	}

	@Test(expectedExceptions = { IllegalStateException.class })
	public void shouldKeepOneToOneSymetric2() {
		initData10();
		OneToOne<Person, Person> rel = db.oneToOneSymmetric(persons, "rel", persons);

		rel.link(1, 2);
		rel.link(5, 2);
	}

	@Test
	public void shouldKeepManyToManySymetric() {
		initData10();
		ManyToMany<Person, Person> rel = db.manyToManySymmetric(persons, "rel", persons);

		rel.link(1, 2);
		rel.link(1, 3);
		rel.link(5, 2);

		System.out.println(rel);
		
		eqs(rel.from(1), 2, 3);
		eqs(rel.from(2), 1, 5);
		eqs(rel.from(3), 1);
		eqs(rel.from(5), 2);

		eqs(rel.to(1), 2, 3);
		eqs(rel.to(2), 1, 5);
		eqs(rel.to(3), 1);
		eqs(rel.to(5), 2);

		haveIds(rel.from(p(1)), 2, 3);
		haveIds(rel.from(p(2)), 1, 5);
		haveIds(rel.from(p(3)), 1);
		haveIds(rel.from(p(5)), 2);

		haveIds(rel.to(p(1)), 2, 3);
		haveIds(rel.to(p(2)), 1, 5);
		haveIds(rel.to(p(3)), 1);
		haveIds(rel.to(p(5)), 2);

		eqs(rel.to(100));
		eqs(rel.from(100));
	}

}
