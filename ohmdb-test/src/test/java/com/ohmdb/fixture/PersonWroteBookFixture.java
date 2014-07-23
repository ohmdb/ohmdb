package com.ohmdb.fixture;

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

import com.ohmdb.abstracts.RelationInternals;
import com.ohmdb.api.Table;
import com.ohmdb.test.Book;
import com.ohmdb.test.Person;
import com.ohmdb.test.TestCommons;
import com.ohmdb.util.UTILS;

public class PersonWroteBookFixture extends TestCommons {

	public Table<Person> persons;
	public Table<Book> books;

	public RelationInternals wrote;
	public RelationInternals writtenBy;

	public long p1;
	public long p2;

	public long b1;
	public long b2;
	public long b3;

	public void init() {
		persons = db.table(Person.class);
		books = db.table(Book.class);

		wrote = (RelationInternals) relation(db, persons, "wrote", books);
		writtenBy = (RelationInternals) wrote.inverse();

		p1 = persons.insert(new Person("Nikolche", 20));
		p2 = persons.insert(new Person("John", 40));

		b1 = books.insert(new Book("Nik Book 1", true));
		b2 = books.insert(new Book("Nik Book 2", false));
		b3 = books.insert(new Book("John Book", true));

		p1().p2().p(0);
	}

	public void p1_x_b1_b2_$_p2_x_b2_b3() {
		init();

		UTILS.link(wrote, p1, nums(b1, b2));
		UTILS.link(wrote, p2, nums(b2, b3));

		check_p1_x_b1_b2_$_p2_x_b2_b3();
	}

	public void check_p1_x_b1_b2_$_p2_x_b2_b3() {
		p1(b1, b2).p2(b2, b3).p(2).b(3);
	}

	public PersonWroteBookFixture p1(long... ids) {
		eqnums(wrote.linksFrom(p1), ids);
		eqnums(writtenBy.linksTo(p1), ids);
		return this;
	}

	public PersonWroteBookFixture p2(long... ids) {
		eqnums(wrote.linksFrom(p2), ids);
		eqnums(writtenBy.linksTo(p2), ids);
		return this;
	}

	public PersonWroteBookFixture b1(long... ids) {
		eqnums(wrote.linksTo(b1), ids);
		eqnums(writtenBy.linksFrom(b1), ids);
		return this;
	}

	public PersonWroteBookFixture b2(long... ids) {
		eqnums(wrote.linksTo(b2), ids);
		eqnums(writtenBy.linksFrom(b2), ids);
		return this;
	}

	public PersonWroteBookFixture b3(long... ids) {
		eqnums(wrote.linksTo(b3), ids);
		eqnums(writtenBy.linksFrom(b3), ids);
		return this;
	}

	public PersonWroteBookFixture p(int size) {
		eq(wrote.fromSize(), size);
		eq(writtenBy.toSize(), size);
		return this;
	}

	public PersonWroteBookFixture b(int size) {
		eq(wrote.toSize(), size);
		eq(writtenBy.fromSize(), size);
		return this;
	}

}
