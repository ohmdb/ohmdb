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

import com.ohmdb.api.OneToMany;
import com.ohmdb.test.Book;
import com.ohmdb.test.Person;
import com.ohmdb.test.TestCommons;

public class AloneRelationsTest extends TestCommons {

	@Test
	public void shouldKeepRelsWithoutTables() {
		initSchema();
		OneToMany<Person, Book> rel = db.oneToMany(null, "rel", null);
		rel.link(1, 2);
		rel.link(1, 3);
		
		isTrue(rel.hasLink(1, 2));
		isTrue(rel.hasLink(1, 3));
		
		isFalse(rel.hasLink(1, 55));
		isFalse(rel.hasLink(22, 3));
		isFalse(rel.hasLink(44, 66));
	}

}
