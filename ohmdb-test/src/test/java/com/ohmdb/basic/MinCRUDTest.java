package com.ohmdb.basic;

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

import com.ohmdb.api.OhmDB;
import com.ohmdb.test.Person;
import com.ohmdb.test.TestCommons;

public class MinCRUDTest extends TestCommons {

	@Test
	public void minimalisticCRUD() {

		/*** INSERT ***/

		Person p1 = new Person("Jane", 32);
		long id1 = OhmDB.insert(p1);
		eq(id1, 0);

		eq(OhmDB.table(Person.class).size(), 1);

		Person p2 = new Person("Mary", 35);
		long id2 = OhmDB.insert(p2);
		eq(id2, 1);

		eq(OhmDB.defaultDb().table(Person.class).size(), 2);

		eq(((Person) OhmDB.get(id1)).name, "Jane");
		eq(((Person) OhmDB.get(id1)).age, 32);

		eq(((Person) OhmDB.get(id2)).name, "Mary");
		eq(((Person) OhmDB.get(id2)).age, 35);

		eq(OhmDB.get(id1), p1);
		eq(OhmDB.get(id2), p2);

		/*** UPDATE ***/

		p1.name = "John";
		p1.age = 27;
		OhmDB.update(p1);

		Person p3 = OhmDB.get(id1);
		eq(OhmDB.get(id1), p1);
		eq(p3.name, "John");
		eq(p3.age, 27);

		eq(OhmDB.defaultDb().table(Person.class).size(), 2);

		/*** DELETE ***/
		OhmDB.delete(id2);
		eq(OhmDB.table(Person.class).size(), 1);

	}

}
