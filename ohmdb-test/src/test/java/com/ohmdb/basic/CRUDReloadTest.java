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

import org.testng.annotations.Test;

import com.ohmdb.api.Ohm;
import com.ohmdb.api.Table;
import com.ohmdb.test.Person;
import com.ohmdb.test.Person2;
import com.ohmdb.test.TestCommons;
import com.ohmdb.util.U;

public class CRUDReloadTest extends TestCommons {

	private Table<Person> persons;
	private Table<Person2> persons2;

	private void reloadAndRefresh() {
		db.shutdown();
		db = Ohm.db(DB_FILE);
		persons = personsTable();
	}

	private void reload2() {
		db.shutdown();
		db = Ohm.db(DB_FILE);
		persons2 = db.table(Person2.class);
	}

	@Test
	public void shouldCRUD() {

		reloadAndRefresh();

		/*** INSERT ***/

		long id0 = persons.insert(person("john", 29));
		reloadAndRefresh();

		long id1 = persons.insert(person("bill", 13));
		reloadAndRefresh();

		eq(persons.size(), 2);

		Person p0 = persons.get(id0);
		eq(p0.name, "john");
		eq(p0.age, 29);

		Person p1 = persons.get(id1);
		eq(p1.name, "bill");
		eq(p1.age, 13);

		/*** SET ***/

		persons.set(id0, "name", "doe");
		reloadAndRefresh();

		Person p2 = persons.get(id0);
		eq(p2.name, "doe");
		eq(p2.age, 29);

		/*** UPDATE ***/

		p0.name = "silvester";
		p0.age = 2;
		persons.update(id0, p0);
		reloadAndRefresh();

		Person p3 = persons.get(id0);
		eq(p3.name, "silvester");
		eq(p3.age, 2);

		/*** UPDATE FROM MAP ***/

		Map<String, Object> map = new HashMap<String, Object>();
		map.put("name", "romeo");
		persons.update(id1, map);
		reloadAndRefresh();

		Person p4 = persons.get(id1);
		eq(p4.name, "romeo");
		eq(p4.age, 13);

		/*** DELETE ***/

		persons.delete(id0);
		reloadAndRefresh();

		eq(persons.size(), 1);

		/*** PRINT ***/

		persons.print();
	}

	@Test
	public void shouldCRUD2() {
		reload2();

		/*** INSERT ***/

		long id0 = persons2.insert(person2("john", 29));
		reload2();

		long id1 = persons2.insert(person2("bill", 13));
		reload2();

		eq(persons2.size(), 2);

		Person2 p0 = persons2.get(id0);
		eq(p0.getName(), "john");
		eq(p0.getAge(), 29);

		Person2 p1 = persons2.get(id1);
		eq(p1.getName(), "bill");
		eq(p1.getAge(), 13);

		/*** SET ***/

		persons2.set(id0, "name", "doe");
		reload2();

		Person2 p2 = persons2.get(id0);
		eq(p2.getName(), "doe");
		eq(p2.getAge(), 29);

		/*** UPDATE ***/

		p0.setName("silvester");
		p0.setAge(2);
		persons2.update(id0, p0);
		reload2();

		Person2 p3 = persons2.get(id0);
		eq(p3.getName(), "silvester");
		eq(p3.getAge(), 2);

		/*** UPDATE FROM MAP ***/

		Map<String, Object> map = new HashMap<String, Object>();
		map.put("name", "romeo");
		persons2.update(id1, map);
		reload2();

		Person2 p4 = persons2.get(id1);
		eq(p4.getName(), "romeo");
		eq(p4.getAge(), 13);

		/*** DELETE ***/

		persons2.delete(id0);
		reload2();

		eq(persons2.size(), 1);

		/*** PRINT ***/

		persons2.print();
	}

	@Test
	public void shouldCRUD3() {

		for (int i = 0; i < 50; i++) {

			U.delete(DB_FILE);
			reloadAndRefresh();

			long id0 = persons.insert(person("john", 29));

			reloadAndRefresh();

			long id1 = persons.insert(person("aaa", 123));

			reloadAndRefresh();

			Person p = new Person();
			p.name = "aaa";
			persons.update(id0, p);

			persons.set(id1, "name", "bbb");

			reloadAndRefresh();

			eq(persons.read(id0, "name"), "aaa");
			eq(persons.get(id0).name, "aaa");

			eq(persons.read(id1, "name"), "bbb");
			eq(persons.get(id1).name, "bbb");

			persons.delete(id0);
			reloadAndRefresh();

			eq(persons.size(), 1);

			eq(persons.read(id1, "name"), "bbb");
			eq(persons.get(id1).name, "bbb");
		}
	}

	@Test
	public void shouldCRUD4() {

		int total = 100;

		for (int i = 0; i < 5; i++) {

			int delN = 0;
			U.delete(DB_FILE);
			reloadAndRefresh();

			for (int j = 0; j < total; j++) {
				persons.insert(person("john", 29));

				try {
					persons.delete(rnd(persons.size() * 2));
					delN++;
				} catch (Exception e) {
				}

				if (rnd(10) > 7) {
					reloadAndRefresh();
				}
			}

			reloadAndRefresh();

			eq(persons.size(), total - delN);
		}
	}

}
