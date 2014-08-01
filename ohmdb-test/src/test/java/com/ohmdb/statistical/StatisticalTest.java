package com.ohmdb.statistical;

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

import com.ohmdb.test.Book;
import com.ohmdb.test.DatabaseCheck;
import com.ohmdb.test.Person;
import com.ohmdb.test.RelationShadow;
import com.ohmdb.test.TableShadow;
import com.ohmdb.test.TestCommons;

public class StatisticalTest extends TestCommons {

	protected static final int MINI_REL_COUNT = 20;

	@Test(dataProvider = "num10")
	public void randomTableAndRelationOps(int scale) {
		final int factor = scale + 1; // [1..11]

		final int total = 100;

		final int threadsFactor = factor * 10; // 1, 2, 3...
		final int refreshMs = 200 * factor + 5; // 5, 25, 55...

		final DatabaseCheck db = new DatabaseCheck(refreshMs, threadsFactor);

		db.register(new TableChecker<Person>("insert person", Person.class) {
			@Override
			public void check(TableShadow<Person> table, int iteration) {
				table.insert(new Person(rndStr(100), rnd()));
			}
		});

		db.register(new TableChecker<Person>("delete person", Person.class) {
			@Override
			public void check(TableShadow<Person> table, int iteration) {
				int id = rnd(iteration);
				table.delete(id);
			}
		});

		db.register(new TableChecker<Person>("set person name", Person.class) {
			@Override
			public void check(TableShadow<Person> table, int iteration) {
				table.set(rnd(total * factor), "name", rndStr(100));
			}
		});

		db.register(new TableChecker<Person>("set person age", Person.class) {
			@Override
			public void check(TableShadow<Person> table, int iteration) {
				table.set(rnd(total * factor), "age", rnd());
			}
		});

		db.register(new TableChecker<Person>("get person", Person.class) {
			@Override
			public void check(TableShadow<Person> table, int iteration) {
				table.get(rnd(total * factor));
			}
		});

		db.register(new TableChecker<Person>("read person name", Person.class) {
			@Override
			public void check(TableShadow<Person> table, int iteration) {
				table.read(rnd(total * factor), "name");
			}
		});

		db.register(new TableChecker<Person>("read person age", Person.class) {
			@Override
			public void check(TableShadow<Person> table, int iteration) {
				table.read(rnd(total * factor), "age");
			}
		});

		db.register(new TableChecker<Book>("insert book", Book.class) {
			@Override
			public void check(TableShadow<Book> table, int iteration) {
				table.insert(new Book(rndStr(100), yesNo()));
			}
		});

		db.register(new TableChecker<Book>("delete book", Book.class) {
			@Override
			public void check(TableShadow<Book> table, int iteration) {
				table.delete(rnd(iteration));
			}
		});

		db.register(new TableChecker<Book>("set book title", Book.class) {
			@Override
			public void check(TableShadow<Book> table, int iteration) {
				table.set(rnd(total * factor), "title", rndStr(100));
			}
		});

		db.register(new TableChecker<Book>("set book published", Book.class) {
			@Override
			public void check(TableShadow<Book> table, int iteration) {
				table.set(rnd(total * factor), "published", yesNo());
			}
		});

		db.register(new TableChecker<Book>("get book", Book.class) {
			@Override
			public void check(TableShadow<Book> table, int iteration) {
				table.get(rnd(total * factor));
			}
		});

		db.register(new TableChecker<Book>("read book published", Book.class) {
			@Override
			public void check(TableShadow<Book> table, int iteration) {
				table.read(rnd(total * factor), "published");
			}
		});

		db.register(new TableChecker<Book>("read book title", Book.class) {
			@Override
			public void check(TableShadow<Book> table, int iteration) {
				table.read(rnd(total * factor), "title");
			}
		});

		db.register(new RelationChecker("link rel1", "rel1") {
			@Override
			public void check(RelationShadow relation, int iteration) {
				relation.link(rnd(iteration * 2), rnd(iteration * 2));
			}
		});

		db.register(new RelationChecker("unlink rel1", "rel1") {
			@Override
			public void check(RelationShadow relation, int iteration) {
				relation.delink(rnd(iteration * 2), rnd(iteration * 2));
			}
		});

		db.register(new RelationChecker("delete from rel1", "rel1") {
			@Override
			public void check(RelationShadow relation, int iteration) {
				relation.deleteFrom(rnd(iteration * 2));
			}
		});

		db.register(new RelationChecker("delete to rel1", "rel1") {
			@Override
			public void check(RelationShadow relation, int iteration) {
				relation.deleteTo(rnd(iteration * 2));
			}
		});

		db.register(new RelationChecker("link rel2", "rel2") {
			@Override
			public void check(RelationShadow relation, int iteration) {
				relation.link(rnd(iteration), rnd(iteration));
			}
		});

		db.register(new RelationChecker("unlink rel2", "rel2") {
			@Override
			public void check(RelationShadow relation, int iteration) {
				relation.delink(rnd(iteration), rnd(iteration));
			}
		});

		db.register(new RelationChecker("delete from rel2", "rel2") {
			@Override
			public void check(RelationShadow relation, int iteration) {
				relation.deleteFrom(rnd(iteration));
			}
		});

		db.register(new RelationChecker("delete to rel2", "rel2") {
			@Override
			public void check(RelationShadow relation, int iteration) {
				relation.deleteTo(rnd(iteration));
			}
		});

		db.register(new RelationChecker("link rel3", "rel3") {
			@Override
			public void check(RelationShadow relation, int iteration) {
				relation.link(rnd(MINI_REL_COUNT), rnd(MINI_REL_COUNT));
			}
		});

		db.register(new RelationChecker("unlink rel3", "rel3") {
			@Override
			public void check(RelationShadow relation, int iteration) {
				relation.delink(rnd(MINI_REL_COUNT), rnd(MINI_REL_COUNT));
			}
		});

		db.register(new RelationChecker("delete from rel3", "rel3") {
			@Override
			public void check(RelationShadow relation, int iteration) {
				relation.deleteFrom(rnd(MINI_REL_COUNT));
			}
		});

		db.register(new RelationChecker("delete to rel3", "rel3") {
			@Override
			public void check(RelationShadow relation, int iteration) {
				relation.deleteTo(rnd(MINI_REL_COUNT));
			}
		});

		db.execute(total);
	}
}
