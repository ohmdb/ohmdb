package com.ohmdb.abstracts;

/*
 * #%L
 * ohmdb-internal-api
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


public interface DbInsider {

	void invalidId(Class<?> table, long id);

	void invalidColumn(Class<?> table, String column);

	void inserting(Class<?> table, long id, Object entity);

	void inserted(Class<?> table, long id, Object entity);

	void insertingCell(Class<?> table, long id, String column, Object value);

	void uninserting(Class<?> table, long id);

	void uninsertingCell(Class<?> table, long id, String column, Object value);

	void changing(Class<?> table, long id, String col, Object value);

	void changed(Class<?> table, long id, String col, Object value);

	void unchanging(Class<?> table, long id, String column, Object oldValue, Object value);

	void deleting(Class<?> table, long id);

	void deletingCell(Class<?> table, long id, String column);

	void deleted(Class<?> table, long id);

	void undeleting(Class<?> table, long id);

	void undeletingCell(Class<?> table, long id, String column);

	void linking(String relation, long from, long to);

	void linked(String relation, long from, long to);

	void delinking(String relation, long from, long to);

	void delinked(String relation, long from, long to);

	void deletingLinksFrom(String relation, long id);

	void deletedLinksFrom(String relation, long id);

	void deletingLinksTo(String relation, long id);

	void deletedLinksTo(String relation, long id);

	void getting(Class<?> table, long id);

	void got(Class<?> table, long id, Object entity);

	void reading(Class<?> table, long id, String column);

	void read(Class<?> table, long id, String column, Object value);

	void unlinking(long fromId, Numbers toIds);

	void undelinking(long fromId, Numbers toIds);

	void undelinking(Numbers fromIds, long toId);

}
