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

import java.util.BitSet;
import java.util.Set;
import java.util.TreeSet;

import com.ohmdb.abstracts.Zones;
import com.ohmdb.util.Check;
import com.ohmdb.util.Errors;

public class ZonesImpl implements Zones {

	// covers a 16 KB block
	private final BitSet bs = new BitSet(16 * 1024 / FileStore.BLOCK_SIZE);

	private int cardinality;

	@Override
	public synchronized Set<Long> occupy(int num) {
		Check.arg(num > 0, "Ocupation size must be greater than 0!");

		return hasFree(num) ? recycle(num) : expand(num);
	}

	private boolean hasFree(int num) {
		return unused() >= num;
	}

	private Set<Long> expand(int num) {
		Set<Long> positions = new TreeSet<Long>();

		for (int pos = bs.length(); pos < bs.length() + num; pos++) {
			positions.add((long) pos);
		}

		bs.set(bs.length(), bs.length() + num);

		cardinality += num;
		assert cardinality == bs.cardinality();

		return positions;
	}

	private Set<Long> recycle(int num) {
		Set<Long> positions = new TreeSet<Long>();

		int n = 0;
		for (int pos = bs.nextClearBit(0); pos >= 0; pos = bs.nextClearBit(pos + 1)) {
			n++;
			positions.add((long) pos);
			bs.set(pos);

			if (n == num) {
				cardinality += num;
				assert cardinality == bs.cardinality();
				return positions;
			}
		}

		throw Errors.notExpected();
	}

	private int unused() {
		return bs.size() - cardinality();
	}

	@Override
	public synchronized void occupied(long position) {
		assert !bs.get((int) position);
		bs.set((int) position);
		cardinality++;
		assert cardinality == bs.cardinality();
	}

	@Override
	public synchronized void release(long position) {
		assert bs.get((int) position);
		bs.clear((int) position);
		cardinality--;
		assert cardinality == bs.cardinality();
	}

	@Override
	public synchronized void releaseAll(long... positions) {
		for (long position : positions) {
			release(position);
		}
	}

	@Override
	public synchronized void releaseAll(Set<Long> positions) {
		for (long position : positions) {
			release(position);
		}
	}

	@Override
	public synchronized void occupiedAll(Set<Long> positions) {
		for (long position : positions) {
			occupied(position);
		}
	}

	@Override
	public synchronized String toString() {
		return "Zones cardinality: " + cardinality;
	}

	public synchronized int cardinality() {
		assert cardinality == bs.cardinality();
		return cardinality;
	}

}
