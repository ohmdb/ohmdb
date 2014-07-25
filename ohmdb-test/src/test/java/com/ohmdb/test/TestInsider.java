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

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Objects;
import com.ohmdb.abstracts.DbInsider;
import com.ohmdb.bean.BeanIntrospector;
import com.ohmdb.bean.PropertyInfo;
import com.ohmdb.numbers.Numbers;
import com.ohmdb.util.Errors;

public class TestInsider implements DbInsider {

	public TestInsider() {
	}

	private final Map<Class<?>, Map<Long, Map<String, Object>>> tables = new HashMap<Class<?>, Map<Long, Map<String, Object>>>();

	private final Map<String, RelationCheck> relations = new HashMap<String, RelationCheck>();

	private final SynchronizationCheck synchro = new SynchronizationCheck();

	private final BeanIntrospector introspector = new BeanIntrospector();

	@Override
	public void invalidId(Class<?> table, long id) {
		check(!table(table).containsKey(id));
	}

	@Override
	public void invalidColumn(Class<?> table, String column) {
		throw Errors.notExpected();
	}

	@Override
	public void inserting(Class<?> table, long id, Object entity) {
		synchro.startWrite(table);
	}

	@Override
	public void inserted(Class<?> table, long id, Object entity) {
		synchro.endWrite(table);
	}

	@Override
	public void insertingCell(Class<?> table, long id, String column, Object value) {
		log("INSERT " + id + "." + column);
		setProp(table, id, column, value);
	}

	@Override
	public void uninserting(Class<?> table, long id) {

	}

	@Override
	public void uninsertingCell(Class<?> table, long id, String column, Object value) {

	}

	@Override
	public void changing(Class<?> table, long id, String col, Object value) {
		synchro.startWrite(table);
	}

	@Override
	public void changed(Class<?> table, long id, String column, Object value) {
		log("CHANGE " + id + "." + column);
		synchro.endWrite(table);

		check(table(table).containsKey(id));
		setProp(table, id, column, value);
	}

	@Override
	public void unchanging(Class<?> table, long id, String column, Object oldValue, Object value) {

	}

	@Override
	public void deleting(Class<?> table, long id) {
		synchro.startWrite(table);
	}

	@Override
	public void deletingCell(Class<?> table, long id, String column) {

	}

	@Override
	public void deleted(Class<?> table, long id) {
		log("DELETE " + id);
		synchro.endWrite(table);

		check(table(table).containsKey(id));
		table(table).remove(id);
	}

	@Override
	public void undeleting(Class<?> table, long id) {

	}

	@Override
	public void undeletingCell(Class<?> table, long id, String column) {

	}

	public Map<Long, Map<String, Object>> getTableValues(Class<?> clazz) {
		return table(clazz);
	}

	private void setProp(Class<?> table, long id, String column, Object value) {
		Map<String, Object> properties = table(table).get(id);

		if (properties == null) {
			properties = new HashMap<String, Object>();
			table(table).put(id, properties);
		}

		properties.put(column, value);
	}

	private void check(boolean condition) {
		assert condition;
		if (!condition) {
			System.out.println("ERROR CONDITION!");
		}
	}

	private Map<Long, Map<String, Object>> table(Class<?> clazz) {
		synchronized (tables) {

			Map<Long, Map<String, Object>> tbl = tables.get(clazz);

			if (tbl == null) {
				tbl = new HashMap<Long, Map<String, Object>>();
				tables.put(clazz, tbl);
			}

			return tbl;
		}
	}

	public RelationCheck rel(String name) {
		synchronized (relations) {
			RelationCheck relation = relations.get(name);

			if (relation == null) {
				relation = new RelationCheck();
				relations.put(name, relation);
			}

			return relation;
		}
	}

	@Override
	public void linking(String relation, long from, long to) {
		synchro.startWrite(relation);
	}

	@Override
	public synchronized void linked(String relation, long from, long to) {
		synchro.endWrite(relation);

		rel(relation).connect(from, to);
	}

	@Override
	public void delinking(String relation, long from, long to) {
		synchro.startWrite(relation);
	}

	@Override
	public synchronized void delinked(String relation, long from, long to) {
		synchro.endWrite(relation);

		rel(relation).disconnect(from, to);
	}

	@Override
	public void deletingLinksFrom(String relation, long id) {
		// synchro.startWrite(relation);
	}

	@Override
	public void deletedLinksFrom(String relation, long id) {
		// synchro.endWrite(relation);

		// rel(relation).deleteFrom(id);
	}

	@Override
	public void deletingLinksTo(String relation, long id) {
		// synchro.startWrite(relation);
	}

	@Override
	public void deletedLinksTo(String relation, long id) {
		// synchro.endWrite(relation);

		// rel(relation).deleteTo(id);
	}

	private void log(String msg) {
		// System.out.println(Thread.currentThread() + " :: " + msg);
	}

	@Override
	public void getting(Class<?> table, long id) {
		synchro.startRead(table);
	}

	@Override
	public void got(Class<?> table, long id, Object entity) {
		PropertyInfo[] properties = introspector.describe(entity.getClass()).getProps();

		Map<String, Object> values = table(table).get(id);
		assert values != null;

		for (PropertyInfo prop : properties) {
			Object value = prop.get(entity);
			Object expected = values.get(prop.getName());

			check(Objects.equal(value, expected));
		}

		synchro.endRead(table);
	}

	@Override
	public void reading(Class<?> table, long id, String column) {
		synchro.startRead(table);
	}

	@Override
	public void read(Class<?> table, long id, String column, Object value) {
		synchro.endRead(table);

		Map<String, Object> values = table(table).get(id);
		Object expected = values.get(column);

		check(Objects.equal(value, expected));
	}

	@Override
	public void unlinking(long fromId, Numbers toIds) {
		// TODO Auto-generated method stub

	}

	@Override
	public void undelinking(long fromId, Numbers toIds) {
		// TODO Auto-generated method stub

	}

	@Override
	public void undelinking(Numbers fromIds, long toId) {
		// TODO Auto-generated method stub

	}

	public void delete(long id) {
		synchronized (relations) {
			for (RelationCheck rel : relations.values()) {
				rel.deleteFrom(id);
				rel.deleteTo(id);
			}
		}
	}

}
