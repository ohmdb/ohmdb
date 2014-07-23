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

public interface Search<E> extends BoundableSearch<E>, Ids<E> {

	<T> Criteria<E, T> and(T column);

	<T> Criteria<E, T> or(T column);

	<T> Criteria<E, T> and(String columnName, Class<T> type);

	<T> Criteria<E, T> or(String columnName, Class<T> type);

	<T> Criteria<E, T> and(CustomIndex<E, T> indexer);

	<T> Criteria<E, T> or(CustomIndex<E, T> indexer);

}
