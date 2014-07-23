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

import java.util.Set;
import java.util.TreeSet;

import com.ohmdb.util.Check;

public class Zones2 {

	private TreeSet<Area> sizy = new TreeSet<Area>(new AreaSpaceComparator());
	private TreeSet<Area> posy = new TreeSet<Area>(new AreaOrderComparator());

	private int areasN = 0;

	public synchronized Set<Long> occupy(int min, int max) {
		Check.arg(min > 0, "Ocupation size must be greater than 0!");

		int NCELLS = Area.SIZE_CELLS;

		if (min <= NCELLS && !sizy.isEmpty()) {
			Area area = sizy.ceiling(new Area(-1, min));

			if (area != null) {
				Set<Long> positions = area.positions();

				if (positions.size() > max) {
					// only part of positions are required
					return area.occupy(max);
				} else {
					remove(area);
					return positions;
				}

			}
		}

		int totalAreas = min / NCELLS;
		if (totalAreas * NCELLS < min) {
			totalAreas++;
		}

		assert totalAreas * NCELLS >= min;

		int base = areasN;
		areasN += totalAreas;

		// System.out.println(">>>> AREAS N = " + areasN);

		long from = base * NCELLS;
		int len = totalAreas * NCELLS;

		Set<Long> positions = new TreeSet<Long>();
		for (int i = 0; i < len; i++) {
			long pos = from++;
			if (i < max) {
				positions.add(pos);
			} else {
				release(pos);
			}
		}

		return positions;
	}

	public synchronized void occupied(long position) {
		int order = (int) (position / Area.SIZE_CELLS); // FIXME too big?

		assert order >= 0;

		if (areasN <= order) {
			areasN = order + 1;
		}

		Area area = posy.ceiling(new Area(order, -1));
		if (area != null && area.order == order) {
			// found area with that order
			remove(area);
		} else {
			area = new Area(order);
			area.fill();
		}

		area.remove(position);
		add(area);
	}

	public synchronized void release(long position) {
		int order = (int) (position / Area.SIZE_CELLS); // FIXME too big?

		// System.out.println("release #" + position + " @" + order);

		assert order >= 0;

		Area area = posy.ceiling(new Area(order, -1));
		if (area != null && area.order == order) {
			// found area with that order
			remove(area);
		} else {
			area = new Area(order);
		}

		area.add(position);
		add(area);
	}

	public synchronized void releaseAll(long... positions) {
		for (long position : positions) {
			release(position);
		}
	}

	public synchronized void releaseAll(Set<Long> positions) {
		for (long position : positions) {
			release(position);
		}
	}

	public synchronized void occupiedAll(Set<Long> positions) {
		for (long position : positions) {
			occupied(position);
		}
	}

	private void remove(Area area) {
		sizy.remove(area);
		posy.remove(area);
	}

	private void add(Area area) {
		sizy.add(area);
		posy.add(area);
	}

	public int zones() {
		return areasN;
	}

	@Override
	public synchronized String toString() {
		return sizy + " :: " + posy;
	}

	public synchronized int areaSize() {
		return Area.SIZE_CELLS;
	}

	public synchronized String size() {
		StringBuffer sb = new StringBuffer();

		for (Area area : sizy) {
			sb.append(area.order);
			sb.append("=");
			sb.append(area.positions.size());
			sb.append(", ");
		}

		return sb.toString();
	}

}
