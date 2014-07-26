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

import com.ohmdb.api.Links;
import com.ohmdb.api.ManyToMany;
import com.ohmdb.api.ManyToOne;
import com.ohmdb.api.OneToMany;
import com.ohmdb.api.OneToOne;
import com.ohmdb.test.Book;
import com.ohmdb.test.Person;
import com.ohmdb.test.TestCommons;
import com.ohmdb.util.UTILS;

public class RelationsCardinalityTest extends TestCommons {

	@Test
	public void shouldKeepOneToMany1() {
		initData10();
		OneToMany<Person, Book> rel = db.oneToMany(persons, "rel", books);

		rel.link(1, 12);
		rel.link(1, 13);

		eqs(rel.from(1), 12, 13);

		eq(rel.to(12), 1);
		eq(rel.to(13), 1);

		haveIds(rel.from(p(1)), 12, 13);
		hasId(rel.to(b(12)), 1);
		hasId(rel.to(b(13)), 1);

		eq(rel.to(1), -1);
		eqs(rel.from(13));

		eq(rel.to(b(1)), null);
		eqs(rel.from(p(9)));

		Book[] toss = rel.from(persons.get(1));
		eq(toss.length, 2);
		eq(toss[0].id, 12);
		eq(toss[1].id, 13);

		Links[] ln1 = db.leftJoin(persons.all(), rel, books.all()).all().links();
		Links exp1 = links(noln(0), ln(1, 12, 13), noln(2), noln(3), noln(4), noln(5), noln(6), noln(7), noln(8),
				noln(9));
		isTrue(UTILS.equal(ln1, nlinks(exp1)));

		Links[] ln2 = db.rightJoin(persons.all(), rel, books.all()).all().links();
		Links exp2 = links(ln(-1, 10, 11, 14, 15, 16, 17, 18, 19), ln(1, 12, 13));
		isTrue(UTILS.equal(ln2, nlinks(exp2)));

		Links[] ln3 = db.fullJoin(persons.all(), rel, books.all()).all().links();
		Links exp3 = links(ln(-1, 10, 11, 14, 15, 16, 17, 18, 19), noln(0), ln(1, 12, 13), noln(2), noln(3), noln(4),
				noln(5), noln(6), noln(7), noln(8), noln(9));
		isTrue(UTILS.equal(ln3, nlinks(exp3)));

	}

	@Test(expectedExceptions = { IllegalStateException.class })
	public void shouldKeepOneToMany2() {
		initData10();
		OneToMany<Person, Book> rel = db.oneToMany(persons, "rel", books);

		rel.link(1, 2);
		rel.link(5, 2);
	}

	@Test
	public void shouldKeepOneToOne1() {
		initData10();
		OneToOne<Person, Book> rel = db.oneToOne(persons, "rel", books);

		rel.link(1, 12);
		rel.link(3, 14);

		eq(rel.from(1), 12);
		eq(rel.to(14), 3);

		hasId(rel.from(p(1)), 12);
		hasId(rel.from(p(3)), 14);

		hasId(rel.to(b(12)), 1);
		hasId(rel.to(b(14)), 3);

		eq(rel.to(1), -1);
		eq(rel.from(12), -1);

		eq(rel.to(b(1)), null);
		eq(rel.from(p(9)), null);
	}

	@Test(expectedExceptions = { IllegalStateException.class })
	public void shouldKeepOneToOne2() {
		initData10();
		OneToOne<Person, Book> rel = db.oneToOne(persons, "rel", books);

		rel.link(1, 2);
		rel.link(1, 3);
	}

	@Test
	public void shouldKeepManyToOne1() {
		initData10();
		ManyToOne<Person, Book> rel = db.manyToOne(persons, "rel", books);

		rel.link(1, 12);
		rel.link(3, 12);

		eqs(rel.to(12), 1, 3);

		eq(rel.from(1), 12);
		eq(rel.from(3), 12);

		haveIds(rel.to(b(12)), 1, 3);

		hasId(rel.from(p(1)), 12);
		hasId(rel.from(p(3)), 12);

		eqs(rel.to(100));
		eq(rel.from(100), -1);

		eqs(rel.to(b(100)));
		eq(rel.from(p(100)), null);
	}

	@Test(expectedExceptions = { IllegalStateException.class })
	public void shouldKeepManyToOne2() {
		initData10();
		ManyToOne<Person, Book> rel = db.manyToOne(persons, "rel", books);

		rel.link(1, 2);
		rel.link(1, 3);
	}

	@Test
	public void shouldKeepManyToMany() {
		initData10();
		ManyToMany<Person, Book> rel = db.manyToMany(persons, "rel", books);

		rel.link(1, 12);
		rel.link(1, 13);
		rel.link(5, 12);

		eqs(rel.from(1), 12, 13);
		eqs(rel.from(5), 12);

		eqs(rel.to(12), 1, 5);
		eqs(rel.to(13), 1);

		haveIds(rel.from(p(1)), 12, 13);
		haveIds(rel.from(p(5)), 12);

		haveIds(rel.to(b(12)), 1, 5);
		haveIds(rel.to(b(13)), 1);

		eqs(rel.to(100));
		eqs(rel.from(100));

		eqs(rel.to(b(100)));
		eqs(rel.from(p(100)));
	}

}
