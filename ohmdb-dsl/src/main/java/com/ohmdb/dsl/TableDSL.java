package com.ohmdb.dsl;

/*
 * #%L
 * ohmdb-dsl
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

import com.ohmdb.abstracts.JokerCreator;
import com.ohmdb.api.Criteria;
import com.ohmdb.api.CustomIndex;
import com.ohmdb.api.Table;
import com.ohmdb.dsl.criteria.impl.ComplexCriteriaImpl;
import com.ohmdb.dsl.criteria.impl.CriteriaImpl;
import com.ohmdb.joker.JokerCreatorImpl;

public abstract class TableDSL<E> implements Table<E> {

	protected final JokerCreator jokerator = new JokerCreatorImpl();

	public <T> Criteria<E, T> where(T property) {
		return new CriteriaImpl<E, T>(this, jokerator.decode(property));
	}

	public <T> Criteria<E, T> where(String propertyName, Class<T> type) {
		return new CriteriaImpl<E, T>(this, propertyName);
	}

	public <T> Criteria<E, T> where(CustomIndex<E, T> indexer) {
		return new ComplexCriteriaImpl<E, T>(this, indexer);
	}

}
