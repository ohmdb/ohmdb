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

import com.ohmdb.abstracts.DbInsider;
import com.ohmdb.numbers.Numbers;

public class IgnorantInsider implements DbInsider {

	@Override
	public void invalidId(Class<?> table, long id) {

	}

	@Override
	public void invalidColumn(Class<?> table, String column) {

	}

	@Override
	public void inserting(Class<?> table, long id, Object entity) {

	}

	@Override
	public void inserted(Class<?> table, long id, Object entity) {

	}

	@Override
	public void insertingCell(Class<?> table, long id, String column, Object value) {

	}

	@Override
	public void uninserting(Class<?> table, long id) {

	}

	@Override
	public void uninsertingCell(Class<?> table, long id, String column, Object value) {

	}

	@Override
	public void changing(Class<?> table, long id, String col, Object value) {

	}

	@Override
	public void changed(Class<?> table, long id, String col, Object value) {

	}

	@Override
	public void unchanging(Class<?> table, long id, String column, Object oldValue, Object value) {

	}

	@Override
	public void deleting(Class<?> table, long id) {

	}

	@Override
	public void deletingCell(Class<?> table, long id, String column) {

	}

	@Override
	public void deleted(Class<?> table, long id) {

	}

	@Override
	public void undeleting(Class<?> table, long id) {

	}

	@Override
	public void undeletingCell(Class<?> table, long id, String column) {

	}

	@Override
	public void linking(String relation, long from, long to) {

	}

	@Override
	public void linked(String relation, long from, long to) {

	}

	@Override
	public void delinking(String relation, long from, long to) {

	}

	@Override
	public void delinked(String relation, long from, long to) {

	}

	@Override
	public void deletingLinksFrom(String relation, long id) {

	}

	@Override
	public void deletedLinksFrom(String relation, long id) {

	}

	@Override
	public void deletingLinksTo(String relation, long id) {

	}

	@Override
	public void deletedLinksTo(String relation, long id) {

	}

	@Override
	public void getting(Class<?> table, long id) {

	}

	@Override
	public void got(Class<?> table, long id, Object entity) {

	}

	@Override
	public void reading(Class<?> table, long id, String column) {

	}

	@Override
	public void read(Class<?> table, long id, String column, Object value) {

	}

	@Override
	public void unlinking(long fromId, Numbers toIds) {

	}

	@Override
	public void undelinking(long fromId, Numbers toIds) {

	}

	@Override
	public void undelinking(Numbers fromIds, long toId) {

	}

}
