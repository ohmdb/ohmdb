package com.ohmdb.dsl.impl;

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

import java.util.Arrays;

import com.ohmdb.api.BoundableSearch;
import com.ohmdb.api.ParameterBinding;
import com.ohmdb.api.Predicate;
import com.ohmdb.api.SearchRetrieval;
import com.ohmdb.api.Table;
import com.ohmdb.util.Errors;
import com.ohmdb.util.UTILS;

public class SearchRetrievalImpl<E> extends AbstractSearchActions<E> implements SearchRetrieval<E> {

	private final AbstractSearchActions<E> search;
	private final Predicate<E> filter;

	public SearchRetrievalImpl(Table<E> table, AbstractSearchActions<E> search, Predicate<E> filter) {
		super(table);
		this.search = search;
		this.filter = filter;
	}

	@Override
	public <T> BoundableSearch<E> bind(ParameterBinding<T> binding) {
		throw Errors.notExpected();
	}

	@Override
	protected long[] findIDs() {
		return UTILS.getIds(findEntities());
	}

	@SuppressWarnings("unchecked")
	@Override
	protected E[] findEntities() {
		E[] entities = search.findEntities();

		int count = 0;
		E[] filtered = (E[]) new Object[entities.length];

		for (E entity : entities) {
			if (filter.test(entity)) {
				filtered[count++] = entity;
			}
		}

		return Arrays.copyOf(filtered, count);
	}

}
