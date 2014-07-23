package com.ohmdb.test;

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

import com.ohmdb.api.Relation;
import com.ohmdb.api.Table;

public class Fixtures extends TestCommons {

	public static void nickN(Table<Person> persons, int n, boolean insert) {
		if (!insert) {
			eq(persons.size(), n);
		}

		for (int i = 0; i < n; i++) {
			Person person = person("nick" + i, i);
			long id = 0 + i;

			if (insert) {
				eq(id, persons.insert(person));
			} else {
				eq(persons.get(id), person);
			}
		}
	}

	public static void bookN(Table<Book> books, int n, boolean insert) {
		if (!insert) {
			eq(books.size(), n);
		}

		for (int i = 0; i < n; i++) {
			Book book = book("book" + i, i % 2 == 0);
			long id = 10 + i;

			if (insert) {
				eq(id, books.insert(book));
			} else {
				eq(books.get(id), book);
			}
		}
	}

	public static void tagN(Table<Tag> tags, int n, boolean insert) {
		if (!insert) {
			eq(tags.size(), n);
		}

		for (int i = 0; i < n; i++) {
			Tag tag = tag("tag" + i);
			long id = 20 + i;

			if (insert) {
				eq(id, tags.insert(tag));
			} else {
				eq(tags.get(id), tag);
			}
		}
	}

	public static void relN(Relation<?, ?> rel, int from1, int from2, int n) {
		for (int i = 0; i < n; i++) {
			rel.link(from1 + i, from2 + i);
		}
	}

}
