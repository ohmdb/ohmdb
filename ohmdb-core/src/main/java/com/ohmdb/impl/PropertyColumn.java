package com.ohmdb.impl;

/*
 * #%L
 * ohmdb-core
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

import java.lang.reflect.Method;
import java.util.List;

import com.ohmdb.abstracts.Column;
import com.ohmdb.util.Errors;

public class PropertyColumn implements Column {

	private final List<?> records;
	private final Method getter;
	private final Method setter;

	public PropertyColumn(Method getter, Method setter, List<?> records) {
		this.getter = getter;
		this.setter = setter;
		this.records = records;
	}

	@Override
	public void set(int row, Object value) {
		Object instance = row(row);
		try {
			setter.invoke(instance, value);
		} catch (Exception e) {
			throw Errors.rte("Cannot get field value!", e);
		}
	}

	@Override
	public Object get(int row) {
		Object instance = row(row);
		try {
			return getter.invoke(instance);
		} catch (Exception e) {
			throw Errors.rte("Cannot get field value!", e);
		}
	}

	@Override
	public Object delete(int row) {
		Object instance = row(row);
		try {
			return getter.invoke(instance);
		} catch (Exception e) {
			throw Errors.rte("Cannot get field value!", e);
		}
	}

	@Override
	public void clear() {
	}

	private Object row(int row) {
		return records.get(row);
	}

}
