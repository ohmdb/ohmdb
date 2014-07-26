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

import java.util.Comparator;
import java.util.Date;

import com.ohmdb.api.BoundableSearch;
import com.ohmdb.api.Predicate;
import com.ohmdb.api.Results;
import com.ohmdb.api.SearchRetrieval;
import com.ohmdb.api.SortedResults;
import com.ohmdb.api.Table;
import com.ohmdb.api.Visitor;
import com.ohmdb.util.Check;

public abstract class AbstractSearchActions<E> implements BoundableSearch<E> {

	protected final Table<E> table;

	public AbstractSearchActions(Table<E> table) {
		this.table = table;
	}

	protected abstract long[] findIDs();

	protected abstract E[] findEntities();

	@Override
	public void print() {
		// FIXME print table header
		long[] ids = findIDs();
		E[] all = table.getAll(ids);

		for (E e : all) {
			System.out.println(e);
		}

		System.out.println();
	}

	@Override
	public void benchmark() {
		long t1 = new Date().getTime();
		long t2 = t1;
		int n = 0;

		while (t2 - t1 < 1000) {
			ids();
			n++;
			t2 = new Date().getTime();
		}

		long dt = t2 - t1;
		long avg = Math.round(n * 1000.0 / dt);
		System.out.println(String.format("Performance of %s: %s times/s", this, avg));
	}

	@Override
	public long[] ids() {
		return findIDs();
	}

	@Override
	public E[] get() {
		long[] ids = findIDs();
		return table.getAll(ids);
	}

	@Override
	public E getOnly() {
		long[] ids = findIDs();
		int size = ids.length;
		Check.state(size == 1, "Expected exactly 1 result, but found: " + size);
		return table.get(ids[0]);
	}

	@Override
	public E getIfExists() {
		long[] ids = findIDs();
		int size = ids.length;
		Check.state(size <= 1, "Expected 0 or 1 results, but found: " + size);
		return size == 1 ? table.get(ids[0]) : null;
	}

	@Override
	public int size() {
		return findIDs().length;
	}

	@Override
	public Results<E> range(int from, int to) {
		return new RangeResults<E>(table, this, from, to);
	}

	@Override
	public Results<E> top(int count) {
		return new RangeResults<E>(table, this, 0, count - 1);
	}

	@Override
	public Results<E> bottom(int count) {
		return new RangeResults<E>(table, this, -count, -1);
	}

	@Override
	public SortedResults<E> sort(Comparator<E> comparator) {
		return new SortedResultsImpl<E>(table, this, comparator);
	}

	@Override
	public SearchRetrieval<E> filter(Predicate<E> filter) {
		return new SearchRetrievalImpl<E>(table, this, filter);
	}

	@Override
	public void each(Visitor<E> visitor) {
		table.forEach(findIDs(), visitor);
	}

}
