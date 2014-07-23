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

import com.ohmdb.api.CustomIndex;
import com.ohmdb.api.Mapper;
import com.ohmdb.test.Person;
import com.ohmdb.test.TestCommons;
import com.ohmdb.util.Measure;

public class ComplexIndexingTest extends TestCommons {

	@Test
	public void shouldIndexWords() {
		initData10();

		CustomIndex<Person, String> words = persons.index(new Mapper<Person, String>() {
			@Override
			public String map(Person p) {
				return p.name + "#" + p.age;
			}
		}, $p.name, $p.age);

		eqs(persons.where(words).eq("nick3#3").ids(), 3);

		eqs(persons.where(words).eq("nick3#3").or(words).gt("nick7").ids(), 3, 7, 8, 9); // nick7#7...

		eqs(persons.where(words).eq("nick3#3").or(words).gte("nick8").ids(), 3, 8, 9);
	}

	@Test
	public void shouldMultiIndexWords() {
		initData10();

		CustomIndex<Person, String> words = persons.multiIndex(new Mapper<Person, String[]>() {
			@Override
			public String[] map(Person p) {
				return (p.name + " " + p.age + " foo").split(" ");
			}
		}, $p.name, $p.age);

		eqs(persons.where(words).eq("foo").ids(), 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
		eqs(persons.where(words).eq("nick3").or(words).eq("5").ids(), 3, 5);
	}

	@Test
	public void shouldMultiIndexNumbers() {
		initData10();

		CustomIndex<Person, Long> nums = persons.multiIndex(new Mapper<Person, Long[]>() {
			@Override
			public Long[] map(Person p) {
				return new Long[] { (long) (p.age * 10), p.id };
			}
		}, $p.id, $p.age);

		eqs(persons.where(nums).eq(4L).or(nums).eq(60L).ids(), 4, 6);
	}

	@Test
	public void shouldIndexFast() {
		initSchemaInMem();
		fillData10();

		persons.createIndexOn($p.name);
		persons.createIndexOn($p.age);

		int count = 100000;

		Measure.start(count);

		for (int i = 0; i < count; i++) {
			persons.set(0, "name", "foo" + i);
		}

		Measure.finish("set string");

		Measure.start(count);

		for (int i = 0; i < count; i++) {
			persons.set(0, "age", i * 10);
		}

		Measure.finish("set int");

		Measure.start(count);

		for (int i = 0; i < count; i++) {
			persons.set(0, "name", "bar" + i);
		}

		Measure.finish("set string again");

	}

	@Test
	public void shouldComplexIndexFast() {
		initSchemaInMem();
		fillData10();

		persons.index(new Mapper<Person, String>() {
			@Override
			public String map(Person p) {
				return p.name + "#" + p.age;
			}
		}, $p.name, $p.age);

		int count = 100000;

		Measure.start(count);

		for (int i = 0; i < count; i++) {
			persons.set(0, "name", "fgg" + i);
		}

		Measure.finish("set string");

		Measure.start(count);

		for (int i = 0; i < count; i++) {
			persons.set(0, "age", i * 10);
		}

		Measure.finish("set int");

	}

}
