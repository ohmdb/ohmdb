package com.ohmdb.join;

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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import com.ohmdb.abstracts.FutureIds;
import com.ohmdb.abstracts.Numbers;
import com.ohmdb.api.JoinMode;
import com.ohmdb.dsl.join.AJoin;
import com.ohmdb.dsl.join.JoinInitializer;
import com.ohmdb.dsl.join.JoinSide;
import com.ohmdb.join.futureid.IDS;
import com.ohmdb.util.Check;
import com.ohmdb.util.Errors;

public class DefaultJoinInitializer implements JoinInitializer, Comparator<JoinSide> {

	public static final DefaultJoinInitializer INSTANCE = new DefaultJoinInitializer();

	private DefaultJoinInitializer() {
	}

	@Override
	public JoinSide[] optimize(AJoin[] joins, FutureIds[] futureIds) {
		JoinSide[] sides = setup(futureIds.length);

		verify(joins, sides);
		init(joins, futureIds, sides);

		// future ids are initialized into sides and changed later
		initialize(joins, sides);

		return sides;
	}

	private void verify(AJoin[] joins, JoinSide[] sides) {
		for (AJoin join : joins) {
			Check.arg(join.from >= 0 && join.to >= 0, "Invalid join: %s", join);
			Check.arg(join.from < sides.length && join.to < sides.length, "Join references unspecified target: %s",
					join);
		}
	}

	private void initialize(AJoin[] joins, JoinSide[] sides) {
		checkIsolated(sides);

		maybeSetIds(sides, joins);

		order(sides, joins);

		transferedPositions(sides, joins);

		initBeforeThem(sides);

		initBridges(sides);
	}

	private JoinSide[] setup(int length) {
		JoinSide[] sides = new JoinSide[length];

		for (int i = 0; i < sides.length; i++) {
			sides[i] = new JoinSide(i);
		}

		return sides;
	}

	private void order(JoinSide[] sides, AJoin[] joins) {
		Arrays.sort(sides, this);

		for (int i = 0; i < sides.length; i++) {
			sides[i].position = i;
		}

		Set<JoinSide> optional = new HashSet<JoinSide>();

		for (int i = 0; i < sides.length; i++) {
			JoinSide side = sides[i];
			if (side.futureIds.optional()) {
				optional.add(side);
				sides[i].position = Integer.MAX_VALUE;
			}
		}

		// System.out.println("NULLED: " + U.textln(nulled.toArray()));

		while (!optional.isEmpty()) {
			int readyCount = sides.length - optional.size();
			JoinSide best = pickBestOptional(optional, sides, readyCount);
			// System.out.println("BEST: " + best);

			optional.remove(best);
			sides[readyCount] = best;
			best.position = readyCount;
		}
	}

	private JoinSide pickBestOptional(Set<JoinSide> optional, JoinSide[] sides, int count) {
		Check.state(sides.length > 0);
		JoinSide best = null;
		int maxFactor = -1;

		for (JoinSide side : optional) {
			int factor = 0;

			for (JoinSide related : side.related) {
				// System.out.println(" - related of " + side.index + " is " +
				// related);
				if (related.position < count) {
					factor++;
				}
			}

			if (maxFactor < factor) {
				maxFactor = factor;
				best = side;
			}

			// System.out.println(" - factor of side:" + side.index + " = " +
			// factor);
		}

		return (maxFactor > 0) ? best : optional.iterator().next();
	}

	private void transferedPositions(JoinSide[] sides, AJoin[] joins) {
		int[] trans = new int[sides.length];

		for (int i = 0; i < sides.length; i++) {
			trans[sides[i].index] = i;
		}

		for (AJoin join : joins) {
			join.from2 = trans[join.from];
			join.to2 = trans[join.to];
		}
	}

	private void init(AJoin[] joins, FutureIds[] futureIds, JoinSide[] sides) {
		for (int i = 0; i < sides.length; i++) {
			JoinSide side = sides[i];

			side.futureIds = futureIds[i];

			for (AJoin join : joins) {
				if (join.from == i && join.to != i) {
					side.froms.add(join);
					side.joins.add(join);
					side.related.add(sides[join.to]);
				} else if (join.to == i && join.from != i) {
					side.tos.add(join);
					side.joins.add(join);
					side.related.add(sides[join.from]);
				} else if (join.from == i && join.to == i) {
					throw Errors.notSupported();
				}
			}
		}
	}

	private void checkIsolated(JoinSide[] sides) {
		for (int i = 0; i < sides.length; i++) {
			if (sides[i].related.isEmpty()) {
				throw Errors.illegalArgument("Isolated (non-related) join target: %s", sides[i].futureIds);
			}
		}
	}

	private boolean maybeSetIds(JoinSide[] sides, AJoin[] joins) {
		int withIds = 0;

		for (JoinSide side : sides) {
			if (!side.futureIds.optional()) {
				withIds++;
			}
		}

		if (withIds == 0) {
			// System.out.println(" ** NO IDS!");
			int minSize = Integer.MAX_VALUE;
			boolean minFrom = false; // whatever
			AJoin minJoin = null; // whatever

			// TODO: optimize for non-inner joins?

			for (AJoin join : joins) {
				int fromN = join.rel.fromSize();
				if (minSize >= fromN && inner(join.mode)) {
					minSize = fromN;
					minJoin = join;
					minFrom = true;
				}

				int toN = join.rel.toSize();
				if (minSize >= toN && inner(join.mode)) {
					minSize = toN;
					minJoin = join;
					minFrom = false;
				}
			}

			if (minJoin != null) {
				if (minFrom) {
					// System.out.println("MIN FROM JOIN " + minJoin +
					// ", MIN SIZE: " + minSize + " :: " + minFrom);
					Numbers ids = minJoin.rel.froms();
					sides[minJoin.from].futureIds = IDS.futureIds(ids);
				} else {
					// System.out.println("MIN TO JOIN " + minJoin +
					// ", MIN SIZE: " + minSize + " :: " + minFrom);
					Numbers ids = minJoin.rel.tos();
					sides[minJoin.to].futureIds = IDS.futureIds(ids);
				}
			} else {
				return false;
			}
		}

		return true;
	}

	private void initBeforeThem(JoinSide[] sides) {
		for (int i = 0; i < sides.length; i++) {
			JoinSide side = sides[i];
			for (JoinSide related : side.related) {
				if (related.position < side.position) {
					side.before.add(related);
				}
			}
		}
	}

	private void initBridges(JoinSide[] sides) {
		for (int i = 0; i < sides.length; i++) {
			JoinSide side = sides[i];

			Set<JoinBridgeRel> fromRels = new HashSet<JoinBridgeRel>();
			Set<JoinBridgeRel> toRels = new HashSet<JoinBridgeRel>();

			for (AJoin join : side.froms) {
				for (JoinSide pre : side.before) {
					if (pre.index == join.to && innerOrRight(join.mode)) {
						// System.out.println("FROM :::::::::::::::::::::: " +
						// join + " :: " + pre);
						fromRels.add(new JoinBridgeRel(join.rel, join.to2, join.mode));
					}
				}
			}

			for (AJoin join : side.tos) {
				for (JoinSide pre : side.before) {
					if (pre.index == join.from && innerOrLeft(join.mode)) {
						// System.out.println("TO :::::::::::::::::::::: " +
						// join + " :: " + pre);
						toRels.add(new JoinBridgeRel(join.rel, join.from2, join.mode));
					}
				}
			}

			side.bridge = new JoinBridgeImpl(fromRels, toRels);
		}
	}

	private boolean inner(JoinMode mode) {
		return mode == JoinMode.INNER;
	}

	private boolean innerOrRight(JoinMode mode) {
		return mode == JoinMode.INNER || mode == JoinMode.RIGHT_OUTER;
	}

	private boolean innerOrLeft(JoinMode mode) {
		return mode == JoinMode.INNER || mode == JoinMode.LEFT_OUTER;
	}

	@Override
	public int compare(JoinSide s1, JoinSide s2) {
		return weight(s1.futureIds) - weight(s2.futureIds);
	}

	private int weight(FutureIds futureIds) {
		int weight = Math.min(futureIds.size(), 1000000000);

		if (futureIds.optional()) {
			weight += 1000000000;
		}

		return weight;
	}

}
