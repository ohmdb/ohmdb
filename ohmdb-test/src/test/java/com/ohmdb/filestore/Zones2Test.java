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

import java.util.Set;

import org.testng.annotations.Test;

import com.ohmdb.test.TestCommons;
import com.ohmdb.util.Measure;

public class Zones2Test extends TestCommons {

	private static final int MAX = Integer.MAX_VALUE;

	@Test
	public void shoudOccupyAndFreeZones() {
		Zones2 zone = new Zones2();

		// Set<Long> released = new HashSet<Long>();

		// FIXME finish the test
		for (int i = 0; i < 100; i++) {
			int n = rnd(30000);
			Set<Long> positions = zone.occupy(n, n * 2);
			isTrue(positions.size() >= n);
			for (long pos : positions) {
				if (rnd(2) == 0) {
					zone.release(pos);
					// released.add(pos);
				}
			}
		}
	}

	@Test
	public void shoudPerformWell() {
		Zones2 zone = new Zones2();

		int total = 10;

		Measure.start(total);

		for (int i = 0; i < total; i++) {
			Set<Long> positions = zone.occupy(100000, 100000);
			for (long pos : positions) {
				zone.release(pos);
			}
		}

		Measure.finish();

	}

	@Test
	public void shoudOccupyOptimal() {
		Zones2 zone = new Zones2();

		zone.releaseAll(10, 20);
		zone.releaseAll(100001, 100002, 100003);

		Set<Long> pos = zone.occupy(1, MAX);
		eq(pos.size(), 2);

		zone.release(20);

		Set<Long> pos2 = zone.occupy(2, MAX);
		eq(pos2.size(), 3);

		zone.release(100001);

		Set<Long> pos3 = zone.occupy(1, MAX);
		eq(pos3.size(), 1);

		Set<Long> pos4 = zone.occupy(1, MAX);
		eq(pos4.size(), 1);

	}

	@Test
	public void shoudOccupyOptimalWithLimit() {
		Zones2 zone = new Zones2();

		zone.releaseAll(10, 20, 30);

		eq(zone.occupy(1, 2).size(), 2);

		zone.release(40);

		eq(zone.occupy(1, 5).size(), 2);

		eq(zone.occupy(1, 1000000).size(), zone.areaSize());
	}

	@Test
	public void shoudOccupyExpand() {
		Zones2 zone = new Zones2();

		eq(zone.occupy(1, 1000000).size(), zone.areaSize());
	}

	@Test
	public void shoudOccupyExpandWithLimit() {
		Zones2 zone = new Zones2();

		eq(zone.occupy(1, 10).size(), 10);

		eq(zone.occupy(1, MAX).size(), zone.areaSize() - 10);
	}

	@Test
	public void shoudFillFromOcupied() {
		Zones2 zone = new Zones2();

		zone.occupied(123);
		zone.occupied(124);

		Set<Long> slots = zone.occupy(1, MAX);
		eq(slots.size(), zone.areaSize() - 2);

		isTrue(slots.contains(0L));
		isTrue(slots.contains(100L));
		isTrue(slots.contains(200L));
		isTrue(slots.contains((long) zone.areaSize() - 1));

		isFalse(slots.contains(123L));
		isFalse(slots.contains(124L));
	}

	@Test
	public void shoudOccupyInOrder() {
		Zones2 zone = new Zones2();

		for (int i = 0; i < 1000; i++) {
			Set<Long> positions = zone.occupy(1, 1);
			eq(positions.size(), 1);

			long pos = positions.iterator().next();
			eq(pos, i);
		}
	}

	@Test
	public void shoudOccupyInOrderD() {
		Zones2 zone = new Zones2();

		for (int i = 0; i < 30000; i++) {
			Set<Long> positions = zone.occupy(1, 1);
			eq(positions.size(), 1);

			long pos = positions.iterator().next();
			eq(pos, i);
		}
	}
	
}
