package com.ohmdb.filestore;

/*
 * #%L
 * ohmdb-test
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

import java.util.HashSet;
import java.util.Set;

import org.testng.annotations.Test;

import com.ohmdb.test.TestCommons;
import com.ohmdb.util.Measure;
import com.ohmdb.util.U;

public class Zones2Test extends TestCommons {

	@Test
	public void shoudOccupyAndReleaseSlots() {
		Zones zone = new Zones();

		Set<Long> occupied = new HashSet<Long>();

		int total = 0;

		for (int i = 0; i < 100; i++) {
			int n = (rnd(10000) + 1) * 2;
			Set<Long> released = new HashSet<Long>();

			Set<Long> positions = zone.occupy(n);
			eq(positions.size(), n);

			occupied.addAll(positions);
			total += positions.size();

			for (long pos : occupied) {
				if (rnd(2) == 0) {
					zone.release(pos);
					released.add(pos);
					total--;
				}
			}
			occupied.removeAll(released);

			eq(zone.cardinality(), total);
		}
	}

	@Test
	public void shoudPerformWell() {
		Zones zone = new Zones();

		int total = 10;

		Measure.start(total);

		for (int i = 0; i < total; i++) {
			Set<Long> positions = zone.occupy(100000);
			for (long pos : positions) {
				zone.release(pos);
			}
		}

		Measure.finish();
	}

	@Test(enabled = false)
	public void shoudOccupyOptimal() {
		Zones zone = new Zones();

		zone.occupy(1000);

		zone.releaseAll(10, 20);
		zone.releaseAll(101, 102, 103);

		eq(zone.occupy(1), U.set(10L));
		eq(zone.occupy(1), U.set(20L));

		zone.releaseAll(10, 20);

		eq(zone.occupy(2), U.set(10L, 20L));

		zone.releaseAll(10, 20);

		// the first and smallest seq, big enough for 3
		eq(zone.occupy(3), U.set(101L, 102L, 103L));
	}

	@Test
	public void shoudOccupyFreeSlots() {
		Zones zone = new Zones();

		for (int i = 0; i < 1000; i++) {
			eq(zone.occupy(1).size(), 1);
		}
	}

	@Test
	public void shoudFillFromOcupied() {
		Zones zone = new Zones();

		zone.occupied(7);
		zone.occupied(8);
		zone.occupied(10);
		zone.occupied(15);

		eq(zone.occupy(3), U.set(0L, 1L, 2L));
		eq(zone.occupy(1), U.set(3L));
		eq(zone.occupy(2), U.set(4L, 5L));
		eq(zone.occupy(2), U.set(6L, 9L));
		eq(zone.occupy(1), U.set(11L));
		eq(zone.occupy(6), U.set(12L, 13L, 14L, 16L, 17L, 18L));
	}

	@Test
	public void shoudOccupyInOrder() {
		Zones zone = new Zones();

		for (int i = 0; i < 1000; i++) {
			Set<Long> positions = zone.occupy(1);
			eq(positions.size(), 1);

			long pos = positions.iterator().next();
			eq(pos, i);
		}
	}

}
