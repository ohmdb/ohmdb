package com.ohmdb.impl;

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

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.ohmdb.exception.InvalidIdException;
import com.ohmdb.test.Person;
import com.ohmdb.test.TestCommons;

public class DeleteTest extends TestCommons {

	@Test
	public void shouldReuseDeletedRowsAfterCommit() {
		TableImpl<Person> persons = (TableImpl<Person>) personsTable();

		long id1 = persons.insert(person("a", 1));

		AssertJUnit.assertEquals(0L, id1);
		AssertJUnit.assertEquals(1, persons.mem());
		AssertJUnit.assertEquals(1, persons.size());
		AssertJUnit.assertEquals(0, persons.getDeletedCount());
		AssertJUnit.assertEquals(0, persons.getUnusedCount());
		persons.revalidate();
		persons.print();

		long id2 = persons.insert(person("b", 2));

		AssertJUnit.assertEquals(1L, id2);
		AssertJUnit.assertEquals(2, persons.mem());
		AssertJUnit.assertEquals(2, persons.size());
		AssertJUnit.assertEquals(0, persons.getDeletedCount());
		AssertJUnit.assertEquals(0, persons.getUnusedCount());
		persons.revalidate();
		persons.print();

		tx();

		persons.delete(id1);

		commit();

		AssertJUnit.assertEquals(2, persons.mem());
		AssertJUnit.assertEquals(1, persons.size());
		AssertJUnit.assertEquals(1, persons.getDeletedCount());
		AssertJUnit.assertEquals(1, persons.getUnusedCount());
		persons.revalidate();
		persons.print();

		long id3 = persons.insert(person("c", 3));

		AssertJUnit.assertEquals(2L, id3);
		AssertJUnit.assertEquals(2, persons.mem());
		AssertJUnit.assertEquals(2, persons.size());
		AssertJUnit.assertEquals(1, persons.getDeletedCount());
		AssertJUnit.assertEquals(0, persons.getUnusedCount());
		persons.revalidate();
		persons.print();
	}

	@Test
	public void shouldNotReuseDeletedRowsBeforeCommit() {
		TableImpl<Person> persons = (TableImpl<Person>) personsTable();

		long id1 = persons.insert(person("a", 1));

		AssertJUnit.assertEquals(0L, id1);
		AssertJUnit.assertEquals(1, persons.mem());
		AssertJUnit.assertEquals(1, persons.size());
		AssertJUnit.assertEquals(0, persons.getDeletedCount());
		AssertJUnit.assertEquals(0, persons.getUnusedCount());
		persons.revalidate();
		persons.print();

		long id2 = persons.insert(person("b", 2));

		AssertJUnit.assertEquals(1L, id2);
		AssertJUnit.assertEquals(2, persons.mem());
		AssertJUnit.assertEquals(2, persons.size());
		AssertJUnit.assertEquals(0, persons.getDeletedCount());
		AssertJUnit.assertEquals(0, persons.getUnusedCount());
		persons.revalidate();
		persons.print();

		tx();

		persons.delete(id1);

		AssertJUnit.assertEquals(2, persons.mem());
		AssertJUnit.assertEquals(1, persons.size());
		AssertJUnit.assertEquals(1, persons.getDeletedCount());
		AssertJUnit.assertEquals(1, persons.getUnusedCount());
		persons.revalidate();
		persons.print();

		commit();

	}

	@Test(expectedExceptions = { InvalidIdException.class })
	public void shouldInvalidateDeletedID() {
		TableImpl<Person> persons = (TableImpl<Person>) personsTable();
		long id1 = persons.insert(person("a", 1));
		persons.delete(id1);
		persons.delete(id1);
	}

}
