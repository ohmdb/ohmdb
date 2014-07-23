package com.ohmdb.sort;

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

import java.util.Comparator;

import org.testng.annotations.Test;

import com.ohmdb.api.Parameter;
import com.ohmdb.api.Predicate;
import com.ohmdb.api.Visitor;
import com.ohmdb.test.Person;
import com.ohmdb.test.TestCommons;

public class SortTest extends TestCommons {

	@Test
	public void shouldMatchByCriteriaConjunction() {
		initData10();

		Comparator<Person> cmp = new Comparator<Person>() {
			@Override
			public int compare(Person p1, Person p2) {
				return p2.age - p1.age;
			}
		};

		Predicate<Person> even = new Predicate<Person>() {
			@Override
			public boolean test(Person p) {
				return p.age % 2 == 0;
			}
		};

		Predicate<Person> gt5 = new Predicate<Person>() {
			@Override
			public boolean test(Person p) {
				return p.age > 5;
			}
		};

		eqnums(persons.where($p.age).gt(3).filter(even).sort(cmp).ids(), 8, 6, 4);

		eqnums(persons.where($p.age).gt(3).filter(even).sort(cmp).range(1, 555).ids(), 6, 4);

		Parameter<Integer> g = db.param("g", Integer.class);
		eqnums(persons.where($p.age).gt(g).bind(g.as(1)).filter(even).filter(gt5).sort(cmp).ids(), 8, 6);

		Visitor<Person> visitor = new Visitor<Person>() {
			@Override
			public boolean visit(Person p) {
				p.age += 100;
				return true;
			}
		};

		persons.where($p.age).gt(3).each(visitor);
		persons.print();

		eqnums(persons.where($p.age).gt(100).range(0, 3).ids(), 4, 5, 6, 7);
		eqnums(persons.where($p.age).gt(100).range(0, -1).ids(), 4, 5, 6, 7, 8, 9);
		eqnums(persons.where($p.age).gt(100).range(-3, -1).ids(), 7, 8, 9);
		eqnums(persons.where($p.age).gt(100).range(-2, -2).ids(), 8);
		eqnums(persons.where($p.age).gt(100).top(3).ids(), 4, 5, 6);
		eqnums(persons.where($p.age).gt(100).bottom(2).ids(), 8, 9);
	}

}
