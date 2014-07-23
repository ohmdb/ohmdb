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

public interface OhmDB {

	<T> Table<T> table(Class<T> clazz);

	<T> Table<T> table(String name);

	<FROM, TO> ManyToOne<FROM, TO> manyToOne(Table<FROM> from, String name, Table<TO> to);

	<FROM, TO> OneToMany<FROM, TO> oneToMany(Table<FROM> from, String name, Table<TO> to);

	<FROM, TO> ManyToMany<FROM, TO> manyToMany(Table<FROM> from, String name, Table<TO> to);

	<FROM_TO> ManyToMany<FROM_TO, FROM_TO> manyToManySymmetric(Table<FROM_TO> from, String name, Table<FROM_TO> to);

	<FROM, TO> OneToOne<FROM, TO> oneToOne(Table<FROM> from, String name, Table<TO> to);

	<FROM_TO> OneToOne<FROM_TO, FROM_TO> oneToOneSymmetric(Table<FROM_TO> from, String name, Table<FROM_TO> to);

	<FROM, TO> Join join(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to);

	<FROM, TO> Join leftJoin(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to);

	<FROM, TO> Join rightJoin(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to);

	<FROM, TO> Join fullJoin(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to);

	<T> TriggerCreator<T> before(Class<T> type);

	<T> TriggerCreator<T> after(Class<T> type);

	<T> void trigger(Class<T> type, TriggerAction action, Trigger<T> trigger);

	Transaction startTransaction();

	void delete(long id);

	void update(Object entity);

	<T> Parameter<T> param(String name, Class<T> type);

	SearchCriteria crit(String columnName, Op op, Object value);

	<T> Ids<T> ids(long... ids);

	<T> Ids<T> all(long... ids);

	void shutdown();

}
