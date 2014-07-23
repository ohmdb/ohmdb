package com.ohmdb.api;

/*
 * #%L
 * ohmdb-api
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

public interface Table<E> extends Ids<E> {

	void clear();

	long insert(E entity);

	long insert(Map<String, Object> columns);

	void update(E entity);

	void update(long id, E entity);

	void update(long id, Map<String, Object> columns);

	void delete(long id);

	void set(long id, String columnName, Object value);

	E get(long id);

	void load(long id, E entity);

	Object read(long id, String columnName);

	void print();

	int size();

	E queryHelper();

	String nameOf(Object column);

	long[] find(SearchCriteria criteria);

	Ids<E> all();

	Ids<E> withIds(long... ids);

	E[] getAll(long... ids);

	String name();

	<T> void createIndexOn(T column);

	<T> void createIndexOn(T column, Transformer<T> transformer);

	void createIndexOnNamed(String columnName);

	<T> void createIndexOnNamed(String columnName, Transformer<T> transformer);

	<T> CustomIndex<E, T> index(Mapper<E, T> mapper, Object... columns);

	<T> CustomIndex<E, T> multiIndex(Mapper<E, T[]> mapper, Object... columns);

	<T> Criteria<E, T> where(T column);

	<T> Criteria<E, T> where(String columnName, Class<T> columnType);

	<T> Criteria<E, T> where(CustomIndex<E, T> index);

	void each(Visitor<E> visitor);

}
