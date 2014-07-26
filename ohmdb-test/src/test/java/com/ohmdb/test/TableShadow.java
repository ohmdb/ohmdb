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

import java.util.Map;
import java.util.Map.Entry;

import com.ohmdb.TableInternals;
import com.ohmdb.abstracts.DbInsider;
import com.ohmdb.api.Table;
import com.ohmdb.bean.BeanIntrospector;
import com.ohmdb.bean.PropertyInfo;
import com.ohmdb.exception.InvalidIdException;
import com.ohmdb.util.ProxyUtil;

public class TableShadow<E> extends TestCommons {

	private Table<E> table;

	private BeanIntrospector intro = new BeanIntrospector();

	private TableInternals<E> internals;

	private PropertyInfo[] props;

	private final TestInsider insider;

	private final Class<E> clazz;

	public TableShadow(Class<E> clazz, TestInsider insider) {
		this.clazz = clazz;
		this.insider = insider;
	}

	@SuppressWarnings({ "unchecked" })
	public void setTable(Table<E> table) {
		this.table = table;
		this.internals = (TableInternals<E>) table;
		this.props = intro.describe(internals.getClazz()).getProps();
		this.internals.setInsider(ProxyUtil.tracer(DbInsider.class, insider));
	}

	public long insert(E entity) {
		return table.insert(entity);
	}

	public void set(long id, String col, Object value) {
		try {
			table.set(id, col, value);
		} catch (InvalidIdException e) {
		}
	}

	public void delete(long id) {
		try {
			table.delete(id);
			insider.delete(id);
		} catch (InvalidIdException e) {
		}
	}

	public void get(long id) {
		try {
			table.get(id);
		} catch (InvalidIdException e) {
		}
	}

	public void read(long id, String col) {
		try {
			table.read(id, col);
		} catch (InvalidIdException e) {
		}
	}

	public Class<E> getClazz() {
		return clazz;
	}

	public void validate() {
		System.out.println(" - validating table for " + clazz);

		Map<Long, Map<String, Object>> values = insider.getTableValues(clazz);

		System.out.println(" -- table size: " + table.size());
		eq(table.size(), values.size());

		for (Entry<Long, Map<String, Object>> entry : values.entrySet()) {
			long id = entry.getKey();
			Map<String, Object> properties = entry.getValue();
			eq(props.length, properties.size());

			E entity = table.get(id);

			for (PropertyInfo prop : props) {
				Object a = prop.get(entity);
				Object b = properties.get(prop.getName());
				eq(a, b);
			}
		}
	}

}
