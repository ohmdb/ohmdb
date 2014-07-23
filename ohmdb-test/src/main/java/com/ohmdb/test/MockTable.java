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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.ohmdb.abstracts.DbInsider;
import com.ohmdb.abstracts.TableInternals;
import com.ohmdb.api.Criteria;
import com.ohmdb.api.CustomIndex;
import com.ohmdb.api.Ids;
import com.ohmdb.api.Mapper;
import com.ohmdb.api.SearchCriteria;
import com.ohmdb.api.Table;
import com.ohmdb.api.Transformer;
import com.ohmdb.api.Trigger;
import com.ohmdb.api.TriggerAction;
import com.ohmdb.api.Visitor;
import com.ohmdb.bean.PropertyInfo;
import com.ohmdb.joker.JokerCreator;

@SuppressWarnings("rawtypes")
public class MockTable implements Table, TableInternals {

	private final String name;
	private final long[] ids;

	public MockTable(String name, long[] ids) {
		this.name = name;
		this.ids = ids;
	}

	@Override
	public long[] ids() {
		return ids;
	}

	@Override
	public String name() {
		return name;
	}

	@Override
	public void fill(long id, String columnName, Object value) {

	}

	@Override
	public ReentrantReadWriteLock getLock() {

		return null;
	}

	@Override
	public void commit() {

	}

	@Override
	public void rollback() {

	}

	@Override
	public Class getClazz() {

		return null;
	}

	@Override
	public void setInsider(DbInsider insider) {

	}

	@Override
	public JokerCreator jokerator() {

		return null;
	}

	@Override
	public void updateObj(Object entity) {

	}

	@Override
	public void addTrigger(TriggerAction action, Trigger trigger) {

	}

	@Override
	public void forEach(Visitor visitor) {

	}

	@Override
	public void forEach(long[] ids, Visitor visitor) {

	}

	@Override
	public void clear() {

	}

	@Override
	public long insert(Object entity) {

		return 0;
	}

	@Override
	public long insert(Map columns) {

		return 0;
	}

	@Override
	public void update(Object entity) {

	}

	@Override
	public void update(long id, Object entity) {

	}

	@Override
	public void update(long id, Map columns) {

	}

	@Override
	public void delete(long id) {

	}

	@Override
	public void set(long id, String columnName, Object value) {

	}

	@Override
	public Object get(long id) {

		return null;
	}

	@Override
	public void load(long id, Object entity) {

	}

	@Override
	public Object read(long id, String columnName) {

		return null;
	}

	@Override
	public void print() {

	}

	@Override
	public int size() {

		return 0;
	}

	@Override
	public Object queryHelper() {

		return null;
	}

	@Override
	public String nameOf(Object column) {

		return null;
	}

	@Override
	public long[] find(SearchCriteria criteria) {

		return null;
	}

	@Override
	public Ids all() {

		return null;
	}

	@Override
	public Ids withIds(long... ids) {

		return null;
	}

	@Override
	public Object[] getAll(long... ids) {

		return null;
	}

	@Override
	public void createIndexOn(Object column) {

	}

	@Override
	public void createIndexOn(Object column, Transformer transformer) {

	}

	@Override
	public void createIndexOnNamed(String columnName) {

	}

	@Override
	public void createIndexOnNamed(String columnName, Transformer transformer) {

	}

	@Override
	public CustomIndex index(Mapper mapper, Object... columns) {

		return null;
	}

	@Override
	public CustomIndex multiIndex(Mapper mapper, Object... columns) {

		return null;
	}

	@Override
	public Criteria where(Object column) {

		return null;
	}

	@Override
	public Criteria where(String columnName, Class columnType) {

		return null;
	}

	@Override
	public Criteria where(CustomIndex index) {

		return null;
	}

	@Override
	public void each(Visitor visitor) {

	}

	@Override
	public PropertyInfo[] props() {

		return null;
	}

}
