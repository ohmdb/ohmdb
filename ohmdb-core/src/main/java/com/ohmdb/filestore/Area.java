package com.ohmdb.filestore;

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

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import com.ohmdb.util.Errors;

public class Area {

	static final int SIZE_MB = 1; // maybe 5?

	// number of cells in one area of size 1MB
	static final int SIZE_CELLS = SIZE_MB * 1024 * 1024 / 64;

	final long order;

	final Set<Long> positions = new TreeSet<Long>();

	private final int length;

	public Area(long order, int length) {
		this.order = order;
		this.length = length;
	}

	public Area(int order) {
		this.order = order;
		this.length = -1;
	}

	public Set<Long> positions() {
		return positions;
	}

	// public Area with(long position) {
	// int len = positions.length;
	// long[] poss = Arrays.copyOf(positions, len + 1);
	// poss[len] = position;
	//
	// return new Area(order, poss);
	// }

	public void add(long position) {
		positions.add(position);
	}

	public int getLength() {
		return length >= 0 ? length : positions.size();
	}

	@Override
	public String toString() {
		return "#" + order + "=" + positions;
	}

	public void remove(long position) {
		positions.remove(position);
	}

	public void fill() {
		long from = order * SIZE_CELLS;
		for (int i = 0; i < SIZE_CELLS; i++) {
			add(from++);
		}
	}

	public Set<Long> occupy(int count) {
		Set<Long> poss2 = new TreeSet<Long>();

		int n = 0;
		Iterator<Long> it = positions.iterator();
		while (it.hasNext()) {
			long pos = it.next();
			n++;

			if (n <= count) {
				poss2.add(pos);
				it.remove();
			} else {
				return poss2;
			}
		}

		throw Errors.notExpected();
	}

}
