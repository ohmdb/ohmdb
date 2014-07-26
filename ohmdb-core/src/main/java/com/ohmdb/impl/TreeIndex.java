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

import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.ohmdb.abstracts.Index;
import com.ohmdb.abstracts.Numbers;
import com.ohmdb.api.Op;
import com.ohmdb.numbers.Nums;
import com.ohmdb.util.Errors;

public class TreeIndex implements Index {

	private TreeMap<Object, SortedSet<Long>> map = new TreeMap<Object, SortedSet<Long>>();

	public TreeIndex() {
	}

	@Override
	public void remove(Object oldValue, long id) {
		SortedSet<Long> set = map.get(oldValue);

		if (set != null) {
			set.remove(new Long(id));
		}
	}

	@Override
	public void add(Object value, long id) {
		SortedSet<Long> set = map.get(value);

		if (set == null) {
			set = new TreeSet<Long>();
			map.put(value, set);
		}

		set.add(id);
	}

	@Override
	public Numbers find(Op op, Object value) {
		switch (op) {
		case EQ:
			SortedSet<Long> ids = map.get(value);
			return ids != null ? Nums.from(ids) : Nums.none();
		case LT:
			return Nums.union(map.headMap(value, false).values());
		case LTE:
			return Nums.union(map.headMap(value, true).values());
		case GT:
			return Nums.union(map.tailMap(value, false).values());
		case GTE:
			return Nums.union(map.tailMap(value, true).values());

		default:
			throw Errors.notExpected();
		}
	}

	@Override
	public void dispose() {
		map = null;
	}

	@Override
	public String toString() {
		// return map.toString();
		return "index";
	}

}
