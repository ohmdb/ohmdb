package com.ohmdb.persist;

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

import java.io.IOException;

import org.testng.annotations.Test;

import com.ohmdb.abstracts.Numbers;
import com.ohmdb.abstracts.RWRelation;
import com.ohmdb.api.Db;
import com.ohmdb.api.Ohm;
import com.ohmdb.api.Table;
import com.ohmdb.test.Book;
import com.ohmdb.test.Person;
import com.ohmdb.test.TestCommons;
import com.ohmdb.util.UTILS;

public class PersistRelationsTest extends TestCommons {

	@Test
	public void shouldPersistRelationsToFile() throws IOException {

		/*** WRITE DB 1 ***/

		Db db = Ohm.db(DB_FILE);

		Table<Person> persons = db.table(Person.class);

		long p1 = persons.insert(person("Nikolche", 20));
		long p2 = persons.insert(person("John", 40));

		Table<Book> books = db.table(Book.class);

		long b1 = books.insert(book("Nik Book 1", true));
		long b2 = books.insert(book("Nik Book 2", false));
		long b3 = books.insert(book("John Book", true));

		RWRelation rel = relation(db, persons, "wrote", books);

		UTILS.link(rel, p1, nums(b1, b2));
		UTILS.link(rel, p2, nums(b2, b3));

		UTILS.delink(rel, p1, nums(b1));

		checkLinks(p1, p2, b1, b2, b3, rel);

		db.shutdown();
		
		/*** READ DB 2 ***/

		Db db2 = Ohm.db(DB_FILE);

		Table<Person> persons2 = db2.table(Person.class);
		Table<Book> books2 = db2.table(Book.class);

		eq(persons2.size(), 2);
		eq(books2.size(), 3);

		RWRelation rel2 = relation(db2, persons2, "wrote", books2);

		checkLinks(p1, p2, b1, b2, b3, rel2);

		db2.shutdown();
	}

	private void checkLinks(long p1, long p2, long b1, long b2, long b3, RWRelation rel) {
		Numbers p1links = rel.linksFrom(p1);
		eqnums(p1links, b2);

		Numbers p2links = rel.linksFrom(p2);
		eqnums(p2links, b2, b3);

		Numbers b1links = rel.linksTo(b1);
		eqnums(b1links);

		Numbers b2links = rel.linksTo(b2);
		eqnums(b2links, p1, p2);

		Numbers b3links = rel.linksTo(b3);
		eqnums(b3links, p2);
	}

}
