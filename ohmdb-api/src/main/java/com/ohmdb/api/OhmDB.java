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

public class OhmDB {

	private static Db DEFAULT_DB = null;

	public static long insert(Object entity) {
		return defaultDb().insert(entity);
	}

	public static void update(Object entity) {
		defaultDb().update(entity);
	}

	public static <T> T get(long id) {
		return defaultDb().get(id);
	}

	public static void delete(long id) {
		defaultDb().delete(id);
	}

	public static void shutdown() {
		defaultDb().shutdown();
	}

	public static <T> TriggerCreator<T> before(Class<T> type) {
		return defaultDb().before(type);
	}

	public static <T> TriggerCreator<T> after(Class<T> type) {
		return defaultDb().after(type);
	}

	public static synchronized Db defaultDb() {
		if (DEFAULT_DB == null) {
			DEFAULT_DB = Ohm.db("ohm.db");
		}
		return DEFAULT_DB;
	}

	public static synchronized void setDefaultDb(Db defaultDb) {
		DEFAULT_DB = defaultDb;
	}

	public static <T> Table<T> table(Class<T> clazz) {
		return defaultDb().table(clazz);
	}

	public static <T> Table<T> table(String name) {
		return defaultDb().table(name);
	}

	public static <FROM, TO> ManyToOne<FROM, TO> manyToOne(Table<FROM> from, String name, Table<TO> to) {
		return defaultDb().manyToOne(from, name, to);
	}

	public static <FROM, TO> OneToMany<FROM, TO> oneToMany(Table<FROM> from, String name, Table<TO> to) {
		return defaultDb().oneToMany(from, name, to);
	}

	public static <FROM, TO> ManyToMany<FROM, TO> manyToMany(Table<FROM> from, String name, Table<TO> to) {
		return defaultDb().manyToMany(from, name, to);
	}

	public static <FROM_TO> ManyToMany<FROM_TO, FROM_TO> manyToManySymmetric(Table<FROM_TO> from, String name,
			Table<FROM_TO> to) {
		return defaultDb().manyToManySymmetric(from, name, to);
	}

	public static <FROM, TO> OneToOne<FROM, TO> oneToOne(Table<FROM> from, String name, Table<TO> to) {
		return defaultDb().oneToOne(from, name, to);
	}

	public static <FROM_TO> OneToOne<FROM_TO, FROM_TO> oneToOneSymmetric(Table<FROM_TO> from, String name,
			Table<FROM_TO> to) {
		return defaultDb().oneToOneSymmetric(from, name, to);
	}

	public static <FROM, TO> Join join(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to) {
		return defaultDb().join(from, relation, to);
	}

	public static <FROM, TO> Join leftJoin(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to) {
		return defaultDb().leftJoin(from, relation, to);
	}

	public static <FROM, TO> Join rightJoin(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to) {
		return defaultDb().rightJoin(from, relation, to);
	}

	public static <FROM, TO> Join fullJoin(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to) {
		return defaultDb().fullJoin(from, relation, to);
	}

	public static <T> void trigger(Class<T> type, TriggerAction action, Trigger<T> trigger) {
		defaultDb().trigger(type, action, trigger);
	}

	public static Transaction startTransaction() {
		return defaultDb().startTransaction();
	}

	public static <T> Parameter<T> param(String name, Class<T> type) {
		return defaultDb().param(name, type);
	}

	public static SearchCriteria crit(String columnName, Op op, Object value) {
		return defaultDb().crit(columnName, op, value);
	}

	public static <T> Ids<T> ids(long... ids) {
		return defaultDb().ids(ids);
	}

	public static <T> Ids<T> all(long... ids) {
		return defaultDb().all(ids);
	}

}
