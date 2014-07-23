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

public interface Criteria<E, T> {

	Search<E> eq(T value);

	Search<E> neq(T value);

	Search<E> lt(T value);

	Search<E> lte(T value);

	Search<E> gt(T value);

	Search<E> gte(T value);

	Search<E> eq(Parameter<T> param);

	Search<E> neq(Parameter<T> param);

	Search<E> lt(Parameter<T> param);

	Search<E> lte(Parameter<T> param);

	Search<E> gt(Parameter<T> param);

	Search<E> gte(Parameter<T> param);

}
