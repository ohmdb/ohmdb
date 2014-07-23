package com.ohmdb.index;

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

import com.ohmdb.api.Transformer;
import com.ohmdb.test.TestCommons;

public class IndexingTest extends TestCommons {

	@Test
	public void shouldIndexOnProperties() {
		initData10();

		persons.createIndexOn($p.age);

		persons.createIndexOn($p.name, new Transformer<String>() {
			@Override
			public String transform(String value) {
				return value != null ? value.toLowerCase() : null;
			}
		});

		eq(persons.where($p.age).eq(3).size(), 1);
		eq(persons.where($p.name).lt("ni").size(), 0);
		eq(persons.where($p.name).lte("NICK2").size(), 3);
		eq(persons.where($p.name).gt("ni").size(), 10);
		eq(persons.where($p.name).gte("NICK9").size(), 1);

		persons.createIndexOn($p.name);

		eq(persons.where($p.age).eq(3).size(), 1);
		eq(persons.where($p.name).lt("ni").size(), 0);
		eq(persons.where($p.name).eq("NICK2").size(), 0);
		eq(persons.where($p.name).gt("ni").size(), 10);
		eq(persons.where($p.name).eq("NICK9").size(), 0);
	}

	@Test(expectedExceptions = { IllegalStateException.class })
	public void shouldFailToSearchWithoutIndex() {
		initSchema();

		persons.where($p.age).eq(1).print();
		persons.createIndexOn($p.age);
	}

	@Test
	public void shouldIndexAfterChange() {
		initSchema();

		persons.insert(person("john", 29));
		persons.createIndexOn($p.age);

		eq(persons.where($p.age).eq(29).size(), 1);
	}

}
