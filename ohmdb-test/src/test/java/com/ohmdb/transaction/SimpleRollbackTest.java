package com.ohmdb.transaction;

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

import com.ohmdb.abstracts.RWRelation;
import com.ohmdb.api.Table;
import com.ohmdb.test.Book;
import com.ohmdb.test.Person;
import com.ohmdb.test.TestCommons;
import com.ohmdb.util.UTILS;

public class SimpleRollbackTest extends TestCommons {

	@Test
	public void shouldRollbackInserts() {
		final Table<Person> persons = personsTable();

		tx();

		eq(persons.size(), 0);

		long id0 = persons.insert(person("john", 29));

		eq(persons.size(), 1);
		eq(id0, 0);

		long id1 = persons.insert(person("bill", 13));

		eq(persons.size(), 2);
		eq(id1, 1);

		rollback();

		eq(persons.size(), 0);

		long id = persons.insert(person("bill", 13));
		eq(id, 0);
	}

	@Test
	public void shouldRollbackPropertyChanges() {
		final Table<Person> persons = personsTable();

		long id0 = persons.insert(person("john", 29));

		eq(persons.read(id0, "name"), "john");

		tx();

		persons.set(id0, "name", "x");
		eq(persons.read(id0, "name"), "x");

		rollback();

		eq(persons.read(id0, "name"), "john");
	}

	@Test
	public void shouldNotRollbackChangesBeforeTransaction() {
		final Table<Person> persons = personsTable();

		long id0 = persons.insert(person("john", 29));
		eq(persons.size(), 1);
		eq(persons.read(id0, "name"), "john");

		tx();
		rollback();

		eq(persons.size(), 1);
		eq(persons.read(id0, "name"), "john");
	}

	@Test
	public void shouldRollbackRelationChanges() {
		Table<Person> persons = personsTable();
		long p1 = persons.insert(person("Nikolche", 20));
		long p2 = persons.insert(person("John", 40));

		Table<Book> books = booksTable();
		long b1 = books.insert(book("Nik Book 1", true));
		long b2 = books.insert(book("Nik Book 2", false));
		long b3 = books.insert(book("John Book", true));

		RWRelation rel = relation(db, persons, "wrote", books);

		UTILS.link(rel, p1, nums(b1, b2));

		System.out.println(rel);

		eq(rel.linksFrom(p1).size(), 2);
		eq(rel.linksFrom(p2).size(), 0);

		// ROLLBACK LINK OPERATION

		tx();

		UTILS.link(rel, p2, nums(b1, b2, b3));

		eq(rel.linksFrom(p1).size(), 2);
		eq(rel.linksFrom(p2).size(), 3);

		System.out.println(rel);

		rollback();

		System.out.println(rel);

		eq(rel.linksFrom(p1).size(), 2);
		eq(rel.linksFrom(p2).size(), 0);

		// ROLLBACK DELINK OPERATION

		tx();

		UTILS.delink(rel, p1, nums(b1, b2));

		eq(rel.linksFrom(p1).size(), 0);
		eq(rel.linksFrom(p2).size(), 0);

		System.out.println(rel);

		rollback();

		System.out.println(rel);

		eq(rel.linksFrom(p1).size(), 2);
		eq(rel.linksFrom(p2).size(), 0);

	}

	@Test
	public void shouldRollbackRelationDeletions() {
		Table<Person> persons = personsTable();
		long p1 = persons.insert(person("Nikolche", 20));
		long p2 = persons.insert(person("John", 40));

		Table<Book> books = booksTable();
		long b1 = books.insert(book("Nik Book 1", true));
		long b2 = books.insert(book("Nik Book 2", false));
		long b3 = books.insert(book("John Book", true));

		RWRelation rel = relation(db, persons, "wrote", books);

		UTILS.link(rel, p1, nums(b1, b2));
		UTILS.link(rel, p2, nums(b1, b2, b3));

		System.out.println(rel);

		eq(rel.linksFrom(p1).size(), 2);
		eq(rel.linksFrom(p2).size(), 3);

		// ROLLBACK DELETE-FROM OPERATION

		tx();

		rel.deleteFrom(p1);

		eq(rel.linksFrom(p1).size(), 0);
		eq(rel.linksFrom(p2).size(), 3);

		System.out.println(rel);

		rollback();

		System.out.println(rel);

		eq(rel.linksFrom(p1).size(), 2);
		eq(rel.linksFrom(p2).size(), 3);

		// ROLLBACK DELETE-TO OPERATION

		tx();

		rel.deleteTo(b2);

		eq(rel.linksFrom(p1).size(), 1);
		eq(rel.linksFrom(p2).size(), 2);

		System.out.println(rel);

		rollback();

		System.out.println(rel);

		eq(rel.linksFrom(p1).size(), 2);
		eq(rel.linksFrom(p2).size(), 3);
	}

}
