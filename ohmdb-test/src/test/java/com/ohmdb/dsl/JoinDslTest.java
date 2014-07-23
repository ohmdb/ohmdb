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

import com.ohmdb.api.Ids;
import com.ohmdb.api.JoinResult;
import com.ohmdb.api.Links;
import com.ohmdb.api.Search;
import com.ohmdb.test.Book;
import com.ohmdb.test.Person;
import com.ohmdb.test.Tag;
import com.ohmdb.test.TestCommons;

// TODO implement UNION of searches

public class JoinDslTest extends TestCommons {

	@Test
	public void shoudQueryOnEmptyData() {
		initSchema();
		initIndexing();

		Search<Person> s1 = persons.where($p.age).eq(2);
		Search<Book> s2 = books.where($b.published).eq(true);

		isFalse(db.join(s1, wrote, s2).exists());
		isFalse(db.join(s2, writtenBy, s1).exists());

		JoinResult jr = db.join(s1, wrote, s2).all();
		Links[] refs = jr.links();

		eq(refs.length, 1);
		eq(refs[0].size(), 0);
	}

	@Test
	public void shoudMatchOn1Join() {
		initData10();

		Search<Person> s1 = persons.where($p.age).gte(7);
		Ids<Book> s2 = books.all();

		JoinResult jr = db.join(s1, wrote, s2).all();
		Links[] refs = jr.links();

		eq(refs.length, 1);

		eqlinks(refs[0], ln(7, 17), ln(8, 18), ln(9, 19));
	}

	@Test
	public void shoudMatchOn2Joins() {
		initData10();

		Search<Person> s1 = persons.where($p.age).gte(7);
		Ids<Book> s2 = books.all();
		Ids<Tag> s3 = tags.withIds(27, 29);

		JoinResult jr = db.join(s1, wrote, s2).join(s2, describedBy, s3).all();
		Links[] refs = jr.links();

		eq(refs.length, 2);
		eqlinks(refs[0], ln(7, 17), ln(9, 19));
		eqlinks(refs[1], ln(17, 27), ln(19, 29));
	}

	@Test
	public void shoudMatchOn3Joins() {
		initData10();

		Search<Person> s1 = persons.where($p.age).gte(7);
		Ids<Book> s2 = books.all();
		Ids<Tag> s3 = tags.withIds(27, 29);

		JoinResult jr = db.join(s1, wrote, s2).join(s2, describedBy, s3).join(s1, follows, s3).all();
		Links[] refs = jr.links();

		eq(refs.length, 3);
		eqlinks(refs[0], ln(7, 17), ln(9, 19));
		eqlinks(refs[1], ln(17, 27), ln(19, 29));
		eqlinks(refs[2], ln(7, 27), ln(9, 29));
	}

	@Test
	public void shoudJoin2TablesA() {
		initData10();

		JoinResult jr = db.join(persons, wrote, books).all();
		Links[] refs = jr.links();

		eq(refs.length, 1);
		eqlinks(refs[0], ln(0, 10), ln(1, 11), ln(2, 12), ln(3, 13), ln(4, 14), ln(5, 15), ln(6, 16), ln(7, 17),
				ln(8, 18), ln(9, 19));
	}

	@Test
	public void shoudJoin2TablesB() {
		initData10();

		JoinResult jr = db.join(persons.all(), wrote, books).all();
		Links[] refs = jr.links();

		eq(refs.length, 1);
		eqlinks(refs[0], ln(0, 10), ln(1, 11), ln(2, 12), ln(3, 13), ln(4, 14), ln(5, 15), ln(6, 16), ln(7, 17),
				ln(8, 18), ln(9, 19));
	}

	@Test
	public void shoudJoin2TablesC() {
		initData10();

		JoinResult jr = db.join(persons, wrote, books.all()).all();
		Links[] refs = jr.links();

		eq(refs.length, 1);
		eqlinks(refs[0], ln(0, 10), ln(1, 11), ln(2, 12), ln(3, 13), ln(4, 14), ln(5, 15), ln(6, 16), ln(7, 17),
				ln(8, 18), ln(9, 19));
	}

	@Test
	public void shoudJoin2TablesD() {
		initData10();

		JoinResult jr = db.join(persons.all(), wrote, books.all()).all();
		Links[] refs = jr.links();

		eq(refs.length, 1);
		eqlinks(refs[0], ln(0, 10), ln(1, 11), ln(2, 12), ln(3, 13), ln(4, 14), ln(5, 15), ln(6, 16), ln(7, 17),
				ln(8, 18), ln(9, 19));
	}

	@Test
	public void shoudJoin3TablesA() {
		initData10();

		JoinResult jr = db.join(persons, wrote, books).join(books, describedBy, tags).join(persons, follows, tags)
				.all();
		Links[] refs = jr.links();

		eq(refs.length, 3);
		eqlinks(refs[0], ln(0, 10), ln(1, 11), ln(2, 12), ln(3, 13), ln(4, 14), ln(5, 15), ln(6, 16), ln(7, 17),
				ln(8, 18), ln(9, 19));
		eqlinks(refs[1], ln(10, 20), ln(11, 21), ln(12, 22), ln(13, 23), ln(14, 24), ln(15, 25), ln(16, 26),
				ln(17, 27), ln(18, 28), ln(19, 29));
		eqlinks(refs[2], ln(0, 20), ln(1, 21), ln(2, 22), ln(3, 23), ln(4, 24), ln(5, 25), ln(6, 26), ln(7, 27),
				ln(8, 28), ln(9, 29));
	}

	@Test
	public void shoudJoin3TablesB() {
		initData10();

		Ids<Person> p = persons.all();
		Ids<Book> b = books.all();
		Ids<Tag> t = tags.all();

		JoinResult jr = db.join(p, wrote, b).join(b, describedBy, t).join(p, follows, t).all();
		Links[] refs = jr.links();

		eq(refs.length, 3);
		eqlinks(refs[0], ln(0, 10), ln(1, 11), ln(2, 12), ln(3, 13), ln(4, 14), ln(5, 15), ln(6, 16), ln(7, 17),
				ln(8, 18), ln(9, 19));
		eqlinks(refs[1], ln(10, 20), ln(11, 21), ln(12, 22), ln(13, 23), ln(14, 24), ln(15, 25), ln(16, 26),
				ln(17, 27), ln(18, 28), ln(19, 29));
		eqlinks(refs[2], ln(0, 20), ln(1, 21), ln(2, 22), ln(3, 23), ln(4, 24), ln(5, 25), ln(6, 26), ln(7, 27),
				ln(8, 28), ln(9, 29));
	}

	@Test
	public void shoudJoin3TablesC() {
		initData10();

		Ids<Person> p = persons.all();
		Ids<Tag> t = tags.all();

		JoinResult jr = db.join(p, wrote, books).join(books, describedBy, t).join(p, follows, t).all();
		Links[] refs = jr.links();

		eq(refs.length, 3);
		eqlinks(refs[0], ln(0, 10), ln(1, 11), ln(2, 12), ln(3, 13), ln(4, 14), ln(5, 15), ln(6, 16), ln(7, 17),
				ln(8, 18), ln(9, 19));
		eqlinks(refs[1], ln(10, 20), ln(11, 21), ln(12, 22), ln(13, 23), ln(14, 24), ln(15, 25), ln(16, 26),
				ln(17, 27), ln(18, 28), ln(19, 29));
		eqlinks(refs[2], ln(0, 20), ln(1, 21), ln(2, 22), ln(3, 23), ln(4, 24), ln(5, 25), ln(6, 26), ln(7, 27),
				ln(8, 28), ln(9, 29));
	}

	@Test
	public void shoudJoin3TablesD() {
		initData10();

		JoinResult jr = db.join(persons.all(), wrote, books.all()).join(books.all(), describedBy, tags.all()).all();
		Links[] refs = jr.links();

		eq(refs.length, 2);
		eqlinks(refs[0], ln(0, 10), ln(1, 11), ln(2, 12), ln(3, 13), ln(4, 14), ln(5, 15), ln(6, 16), ln(7, 17),
				ln(8, 18), ln(9, 19));
		eqlinks(refs[1], ln(10, 20), ln(11, 21), ln(12, 22), ln(13, 23), ln(14, 24), ln(15, 25), ln(16, 26),
				ln(17, 27), ln(18, 28), ln(19, 29));
	}

}
