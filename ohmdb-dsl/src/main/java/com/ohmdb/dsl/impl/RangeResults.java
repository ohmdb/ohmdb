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

import com.ohmdb.api.BoundableSearch;
import com.ohmdb.api.ParameterBinding;
import com.ohmdb.api.Table;
import com.ohmdb.util.Errors;

public class RangeResults<E> extends AbstractSearchActions<E> {

	private final AbstractSearchActions<E> search;
	private final int from;
	private final int to;

	public RangeResults(Table<E> table, AbstractSearchActions<E> search, int from, int to) {
		super(table);
		this.search = search;
		this.from = from;
		this.to = to;
	}

	@Override
	public <T> BoundableSearch<E> bind(ParameterBinding<T> binding) {
		throw Errors.notExpected();
	}

	@Override
	protected long[] findIDs() {
		long[] ids = search.findIDs();

		int start = from >= 0 ? from : ids.length + from;
		int end = to >= 0 ? to : ids.length + to;

		if (start < 0) {
			start = 0;
		}

		if (end > ids.length - 1) {
			end = ids.length - 1;
		}

		if (start > end) {
			return new long[0];
		}

		int size = end - start + 1;

		long[] part = new long[size];
		System.arraycopy(ids, start, part, 0, size);

		return part;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected E[] findEntities() {
		E[] entities = search.findEntities();

		int start = from >= 0 ? from : entities.length + from;
		int end = to >= 0 ? to : entities.length + to;

		if (start < 0) {
			start = 0;
		}

		if (end > entities.length - 1) {
			end = entities.length - 1;
		}

		if (start > end) {
			return (E[]) new Object[0];
		}

		int size = end - start + 1;

		E[] part = (E[]) new Object[size];
		System.arraycopy(entities, start, part, 0, size);

		return part;
	}

}
