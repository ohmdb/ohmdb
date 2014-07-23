package com.ohmdb.dsl;

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

import com.ohmdb.api.Parameter;
import com.ohmdb.test.Fixtures;
import com.ohmdb.test.TestCommons;

public class TableDslTest extends TestCommons {

	@Test
	public void shoudQueryByIntProperty() {
		initSchema();
		initIndexing();
		Fixtures.nickN(persons, 5, true);

		eqs(persons.where($p.age).eq(2).ids(), 2);

		eqs(persons.where($p.age).lt(2).ids(), 0, 1);

		eqs(persons.where($p.age).lte(2).ids(), 0, 1, 2);

		eqs(persons.where($p.age).gt(2).ids(), 3, 4);

		eqs(persons.where($p.age).gte(2).ids(), 2, 3, 4);

		eqs(persons.where($p.age).neq(2).ids(), 0, 1, 3, 4);
	}

	@Test
	public void shoudQueryByIntParam() {
		initSchema();
		initIndexing();
		Fixtures.nickN(persons, 5, true);

		Parameter<Integer> $age = db.param("age", Integer.class);

		eqs(persons.where($p.age).eq($age).bind($age.as(2)).ids(), 2);

		eqs(persons.where($p.age).lt($age).bind($age.as(2)).ids(), 0, 1);

		eqs(persons.where($p.age).lte($age).bind($age.as(2)).ids(), 0, 1, 2);

		eqs(persons.where($p.age).gt($age).bind($age.as(2)).ids(), 3, 4);

		eqs(persons.where($p.age).gte($age).bind($age.as(2)).ids(), 2, 3, 4);

		eqs(persons.where($p.age).neq($age).bind($age.as(2)).ids(), 0, 1, 3, 4);
	}

	@Test(expectedExceptions = { IllegalArgumentException.class })
	public void shoudFailOnUnspecifiedParam() {
		initSchema();
		Fixtures.nickN(persons, 5, true);

		Parameter<Integer> $age = db.param("age", Integer.class);

		persons.where($p.age).neq($age).ids();
	}

	@Test(expectedExceptions = { IllegalArgumentException.class })
	public void shoudFailOnUnspecifiedParam2() {
		initSchema();
		initIndexing();
		Fixtures.nickN(persons, 5, true);

		Parameter<Integer> $age = db.param("age", Integer.class);

		persons.where($p.age).eq($age).and($p.name).eq("f").ids();
	}

	@Test
	public void shoudQueryByIntAndStringProperty() {
		initSchema();
		initIndexing();
		Fixtures.nickN(persons, 5, true);

		eqs(persons.where($p.age).eq(2).and($p.name).eq("nick2").ids(), 2);

		eqs(persons.where($p.age).lt(2).and($p.name).eq("nick1").ids(), 1);

		eqs(persons.where($p.age).lte(2).and($p.name).eq("nick0").ids(), 0);

		eqs(persons.where($p.age).gt(2).and($p.name).eq("nick3").ids(), 3);

		eqs(persons.where($p.age).gte(2).and($p.name).eq("nick4").ids(), 4);

		eqs(persons.where($p.age).neq(2).and($p.name).eq("nick4").ids(), 4);

		eqs(persons.where($p.age).eq(1).and($p.name).eq("nick2").ids());

		eqs(persons.where($p.age).lt(2).and($p.name).eq("nick2").ids());

		eqs(persons.where($p.age).lte(2).and($p.name).eq("nick3").ids());

		eqs(persons.where($p.age).gt(2).and($p.name).eq("nick2").ids());

		eqs(persons.where($p.age).gte(2).and($p.name).eq("nick1").ids());

		eqs(persons.where($p.age).eq(1).and($p.age).eq(2).ids());

		eqs(persons.where($p.age).neq(1).and($p.age).neq(3).ids(), 0, 2, 4);
	}

	@Test
	public void shoudQueryByIntOrStringProperty() {
		initSchema();
		initIndexing();
		Fixtures.nickN(persons, 5, true);

		eqs(persons.where($p.age).eq(2).or($p.name).eq("nick2").ids(), 2);

		eqs(persons.where($p.age).lt(2).or($p.name).eq("nick1").ids(), 0, 1);

		eqs(persons.where($p.age).lte(2).or($p.name).eq("nick4").ids(), 0, 1, 2, 4);

		eqs(persons.where($p.age).gt(2).or($p.name).eq("nick3").ids(), 3, 4);

		eqs(persons.where($p.age).gte(2).or($p.name).eq("nick1").ids(), 1, 2, 3, 4);

		eqs(persons.where($p.age).neq(2).or($p.name).eq("nick2").ids(), 0, 1, 2, 3, 4);

		eqs(persons.where($p.age).eq(1).or($p.age).eq(3).ids(), 1, 3);

		eqs(persons.where($p.age).neq(1).or($p.age).neq(3).ids(), 0, 1, 2, 3, 4);
	}

	@Test
	public void shoudQueryFast() {
		// FIXME: check speed after improvements
		initSchemaInMem();
		initIndexing();
		Fixtures.nickN(persons, 10000, true);

		persons.where($p.age).gt(5000).benchmark();
	}

}
