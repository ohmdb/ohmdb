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
import com.ohmdb.api.Search;
import com.ohmdb.test.Book;
import com.ohmdb.test.Person;
import com.ohmdb.test.Tag;
import com.ohmdb.test.TestCommons;

public class QueryDslTest extends TestCommons {

	@Test
	public void shouldQuery() {
		initSchema();
		initIndexing();

		Person person1 = person("john", 29);
		Person person2 = person("bill", 13);

		persons.insert(person1);
		persons.insert(person2);

		Person p = persons.queryHelper();

		Person p1 = persons.where(p.age).eq(29).getOnly();
		eq(p1.id, person1.id);
		eq(p1.name, person1.name);
		eq(p1.age, person1.age);

		Person p2 = persons.where(p.name).eq("bill").getOnly();
		eq(p2.id, person2.id);
		eq(p2.name, person2.name);
		eq(p2.age, person2.age);
	}

	@Test
	public void shoudDoAllQueryOps() {
		initData10();

		Search<Person> search = persons.where($p.age).lte(1);

		eq(search.size(), 2);

		// FIXME finish
	}

	@Test
	public void shoudDoAllRecordsOps() {
		initData10();

		Ids<Tag> s3 = tags.withIds(27, 29);
		
		// FIXME finish
	}

	@Test
	public void shoudSupportAny() {
		initData10();

		Ids<Book> any = books.all();
		isntNull(any);
		isNull(any.ids());
	}

}
