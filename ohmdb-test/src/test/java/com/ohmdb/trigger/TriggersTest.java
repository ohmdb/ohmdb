package com.ohmdb.trigger;

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

import com.ohmdb.api.TriggerAction;
import com.ohmdb.test.Book;
import com.ohmdb.test.Person;
import com.ohmdb.test.TestCommons;
import com.ohmdb.util.Errors;

public class TriggersTest extends TestCommons {

	@Test
	public void shouldTriggerOnInsert() {
		initData10();

		TestTrigger<Person> trigger1 = trigger();
		TestTrigger<Person> trigger2 = trigger();

		db.before(Person.class).inserted().run(trigger1);
		db.after(Person.class).inserted().run(trigger2);

		eq(trigger1.counter(), 0);
		eq(trigger2.counter(), 0);

		persons.insert(person("nick", 55));

		eq(trigger1.counter(), 1);
		eq(trigger2.counter(), 1);
	}

	@Test
	public void shouldTriggerOnUpdate() {
		initData10();

		TestTrigger<Person> trigger1 = trigger();
		TestTrigger<Person> trigger2 = trigger();

		db.before(Person.class).updated().run(trigger1);
		db.after(Person.class).updated().run(trigger2);

		eq(trigger1.counter(), 0);
		eq(trigger2.counter(), 0);

		persons.set(0, "name", "nn");

		eq(trigger1.counter(), 1);
		eq(trigger2.counter(), 1);
	}

	@Test
	public void shouldTriggerOnDelete() {
		initData10();

		TestTrigger<Person> trigger1 = trigger();
		TestTrigger<Person> trigger2 = trigger();

		db.before(Person.class).deleted().run(trigger1);
		db.after(Person.class).deleted().run(trigger2);

		eq(trigger1.counter(), 0);
		eq(trigger2.counter(), 0);

		persons.delete(1);

		eq(trigger1.counter(), 1);
		eq(trigger2.counter(), 1);
	}

	@Test
	public void shouldTriggerOnRead() {
		initData10();

		TestTrigger<Person> trigger1 = trigger();
		TestTrigger<Person> trigger2 = trigger();

		db.before(Person.class).read().run(trigger1);
		db.after(Person.class).read().run(trigger2);

		eq(trigger1.counter(), 0);
		eq(trigger2.counter(), 0);

		persons.get(1);

		eq(trigger1.counter(), 1);
		eq(trigger2.counter(), 1);
	}

	@Test
	public void shouldTriggerOnSuperClasses() {
		initData10();

		TestTrigger<Object> trigger1 = trigger();
		TestTrigger<Object> trigger2 = trigger();

		db.before(Object.class).inserted().run(trigger1);
		db.after(Object.class).inserted().run(trigger2);

		eq(trigger1.counter(), 0);
		eq(trigger2.counter(), 0);

		persons.insert(person("nick", 55));

		eq(trigger1.counter(), 1);
		eq(trigger2.counter(), 1);
	}

	@Test
	public void shouldTriggerOnOtherClasses() {
		initData10();

		TestTrigger<Book> trigger1 = trigger();
		TestTrigger<Book> trigger2 = trigger();

		db.before(Book.class).inserted().run(trigger1);
		db.after(Book.class).inserted().run(trigger2);

		eq(trigger1.counter(), 0);
		eq(trigger2.counter(), 0);

		persons.insert(person("nick", 55));

		eq(trigger1.counter(), 0);
		eq(trigger2.counter(), 0);
	}

	@Test
	public void shouldRollbackOnTriggerFailureBeforeOp() {
		initData10();

		db.before(Person.class).inserted().run(failer());

		tx();

		try {
			persons.insert(person("nick", 55));
		} catch (Exception e) {
			checkData10(); // failed before any change
			rollback();
			checkData10();
			return;
		}

		throw Errors.notExpected();
	}

	@Test
	public void shouldRollbackOnTriggerFailureAfterOp() {
		initData10();

		db.after(Person.class).inserted().run(failer());

		tx();

		try {
			persons.insert(person("nick", 55));
		} catch (Exception e) {
			rollback();
			checkData10();
			return;
		}

		throw Errors.notExpected();
	}

	private <K> TestTrigger<K> trigger() {
		return new TestTrigger<K>();
	}

	private TestTrigger<Person> failer() {
		return new TestTrigger<Person>() {
			@Override
			public void process(TriggerAction action, long id, Person oldEntity, Person newEntity) {
				throw Errors.rte(null);
			}
		};
	}

}
