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

import java.util.HashMap;
import java.util.Map;

import com.ohmdb.test.Tag;
import org.testng.annotations.Test;

import com.ohmdb.api.OhmDB;
import com.ohmdb.api.Table;
import com.ohmdb.test.Person;
import com.ohmdb.test.Person2;
import com.ohmdb.test.TestCommons;

public class CRUDTest extends TestCommons {

	@Test
	public void shouldCRUD() {
		Table<Person> persons = personsTable();

		/*** INSERT ***/

		long id0 = persons.insert(person("john", 29));

		long id1 = db.insert(person("bill", 13));

		eq(persons.size(), 2);

		Person p0 = OhmDB.get(id0);
		eq(p0.name, "john");
		eq(p0.age, 29);

		Person p1 = persons.get(id1);
		eq(p1.name, "bill");
		eq(p1.age, 13);

		/*** SET ***/

		persons.set(id0, "name", "doe");

		Person p2 = db.get(id0);
		eq(p2.name, "doe");
		eq(p2.age, 29);

		/*** UPDATE ***/

		p0.name = "silvester";
		p0.age = 2;
		persons.update(id0, p0);

		Person p3 = persons.get(id0);
		eq(p3.name, "silvester");
		eq(p3.age, 2);

		/*** UPDATE FROM MAP ***/

		Map<String, Object> map = new HashMap<String, Object>();
		map.put("name", "romeo");
		persons.update(id1, map);

		Person p4 = persons.get(id1);
		eq(p4.name, "romeo");
		eq(p4.age, 13);

		/*** DELETE ***/

		persons.delete(id0);
		eq(persons.size(), 1);

		/*** PRINT ***/

		persons.print();
	}

	@Test
	public void shouldCRUD2() {
		Table<Person2> persons = db.table(Person2.class);

		/*** INSERT ***/

		long id0 = persons.insert(person2("john", 29, new Tag("architect"), new Tag("fashionista")));
		long id1 = persons.insert(person2("bill", 13, new Tag("astronaut")));

		eq(persons.size(), 2);

		Person2 p0 = persons.get(id0);
		eq(p0.getName(), "john");
		eq(p0.getAge(), 29);
		eqs(p0.getTags(), new Tag[] { new Tag("architect"), new Tag("fashionista") });

		Person2 p1 = persons.get(id1);
		eq(p1.getName(), "bill");
		eq(p1.getAge(), 13);
		eqs(p1.getTags(), new Tag[] { new Tag("astronaut") });

		/*** SET ***/

		persons.set(id0, "name", "doe");

		Person2 p2 = persons.get(id0);
		eq(p2.getName(), "doe");
		eq(p2.getAge(), 29);
		eqs(p2.getTags(), new Tag[] { new Tag("architect"), new Tag("fashionista") });

		/*** UPDATE ***/

		p0.setName("silvester");
		p0.setAge(2);
		p0.setTags(new Tag("pilot"));
		persons.update(id0, p0);

		Person2 p3 = persons.get(id0);
		eq(p3.getName(), "silvester");
		eq(p3.getAge(), 2);
		eqs(p3.getTags(), new Tag[] { new Tag("pilot") });

		/*** UPDATE FROM MAP ***/

		Map<String, Object> map = new HashMap<String, Object>();
		map.put("name", "romeo");
		persons.update(id1, map);

		Person2 p4 = persons.get(id1);
		eq(p4.getName(), "romeo");
		eq(p4.getAge(), 13);

		/*** DELETE ***/

		persons.delete(id0);
		eq(persons.size(), 1);

		/*** PRINT ***/

		persons.print();
	}

}
