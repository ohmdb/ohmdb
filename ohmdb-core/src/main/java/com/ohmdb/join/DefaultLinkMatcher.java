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

import com.ohmdb.abstracts.FutureIds;
import com.ohmdb.abstracts.Numbers;
import com.ohmdb.abstracts.ReadOnlyRelation;
import com.ohmdb.api.Links;
import com.ohmdb.dsl.join.AJoin;
import com.ohmdb.dsl.join.JoinConfig;
import com.ohmdb.dsl.join.JoinSide;
import com.ohmdb.dsl.join.LinkMatcher;
import com.ohmdb.links.LinksBuilder;
import com.ohmdb.util.Errors;
import com.ohmdb.util.U;
import com.ohmdb.util.UTILS;

public class DefaultLinkMatcher implements LinkMatcher {

	public DefaultLinkMatcher() {
	}

	@Override
	public Links[] match(JoinConfig config) {
		Links[] links = new Links[config.getJoins().length];

		doMatch(config, links, false);

		return links;
	}

	@Override
	public boolean exists(JoinConfig config) {
		boolean exists = doMatch(config, null, true);

		return exists;
	}

	private boolean doMatch(JoinConfig config, Links[] links, boolean exists) {
		AJoin[] joins = config.getJoins();

		JoinSide[] sides = config.sides();

		int joinSize = joins.length;
		int tableSize = sides.length;

		LinksBuilder[] builders = null;
		if (!exists) {
			builders = new LinksBuilder[joinSize];

			for (int i = 0; i < builders.length; i++) {
				builders[i] = UTILS.linkBuilder();
			}
		}

		boolean matched;
		switch (tableSize) {
		case 2:
			matched = match2(joins, builders, sides, exists);
			break;

		case 3:
			matched = match3(joins, builders, sides, exists);
			break;

		case 4:
			matched = match4(joins, builders, sides, exists);
			break;

		case 5:
			matched = match5(joins, builders, sides, exists);
			break;

		default:
			throw Errors.notSupported();
		}

		if (!exists) {
			for (int i = 0; i < links.length; i++) {
				links[i] = builders[i].build();
			}
		}

		return matched;
	}

	private boolean match2(AJoin[] joins, LinksBuilder[] builders, JoinSide[] sides, boolean exists) {
		long[] combo = new long[2];
		JoinSide side1 = sides[1];
		Numbers src0 = sides[0].futureIds.fetch();

		for (int i0 = -1; i0 < src0.size(); i0++) {
			combo[0] = i0 < 0 ? -1 : src0.at(i0);
			Numbers src1 = side1.bridge.reach(combo, 1, sides[1].futureIds);

			for (int i1 = -1; i1 < src1.size(); i1++) {
				combo[1] = i1 < 0 ? -1 : src1.at(i1);

				if (check(joins, combo, builders, sides, exists) && exists) {
					return true;
				}
			}
		}

		return false;
	}

	private boolean match3(AJoin[] joins, LinksBuilder[] builders, JoinSide[] sides, boolean exists) {
		long[] combo = new long[3];
		JoinSide side1 = sides[1], side2 = sides[2];
		Numbers src0 = sides[0].futureIds.fetch();

		for (int i0 = -1; i0 < src0.size(); i0++) {
			combo[0] = i0 < 0 ? -1 : src0.at(i0);
			Numbers src1 = side1.bridge.reach(combo, 1, sides[1].futureIds);

			for (int i1 = -1; i1 < src1.size(); i1++) {
				combo[1] = i1 < 0 ? -1 : src1.at(i1);
				Numbers src2 = side2.bridge.reach(combo, 2, sides[2].futureIds);

				for (int i2 = -1; i2 < src2.size(); i2++) {
					combo[2] = i2 < 0 ? -1 : src2.at(i2);

					if (check(joins, combo, builders, sides, exists) && exists) {
						return true;
					}
				}
			}
		}

		return false;
	}

	private boolean match4(AJoin[] joins, LinksBuilder[] builders, JoinSide[] sides, boolean exists) {
		long[] combo = new long[4];
		JoinSide side1 = sides[1], side2 = sides[2], side3 = sides[3];
		Numbers src0 = sides[0].futureIds.fetch();

		for (int i0 = -1; i0 < src0.size(); i0++) {
			combo[0] = i0 < 0 ? -1 : src0.at(i0);
			Numbers src1 = side1.bridge.reach(combo, 1, sides[1].futureIds);

			for (int i1 = -1; i1 < src1.size(); i1++) {
				combo[1] = i1 < 0 ? -1 : src1.at(i1);
				Numbers src2 = side2.bridge.reach(combo, 2, sides[2].futureIds);

				for (int i2 = -1; i2 < src2.size(); i2++) {
					combo[2] = i2 < 0 ? -1 : src2.at(i2);
					Numbers src3 = side3.bridge.reach(combo, 3, sides[3].futureIds);

					for (int i3 = -1; i3 < src3.size(); i3++) {
						combo[3] = i3 < 0 ? -1 : src3.at(i3);

						if (check(joins, combo, builders, sides, exists) && exists) {
							return true;
						}
					}
				}
			}
		}

		return false;
	}

	private boolean match5(AJoin[] joins, LinksBuilder[] builders, JoinSide[] sides, boolean exists) {
		long[] combo = new long[5];
		JoinSide side1 = sides[1], side2 = sides[2], side3 = sides[3], side4 = sides[4];
		Numbers src0 = sides[0].futureIds.fetch();

		for (int i0 = -1; i0 < src0.size(); i0++) {
			combo[0] = i0 < 0 ? -1 : src0.at(i0);
			Numbers src1 = side1.bridge.reach(combo, 1, sides[1].futureIds);

			for (int i1 = -1; i1 < src1.size(); i1++) {
				combo[1] = i1 < 0 ? -1 : src1.at(i1);
				Numbers src2 = side2.bridge.reach(combo, 2, sides[2].futureIds);

				for (int i2 = -1; i2 < src2.size(); i2++) {
					combo[2] = i2 < 0 ? -1 : src2.at(i2);
					Numbers src3 = side3.bridge.reach(combo, 3, sides[3].futureIds);

					for (int i3 = -1; i3 < src3.size(); i3++) {
						combo[3] = i3 < 0 ? -1 : src3.at(i3);
						Numbers src4 = side4.bridge.reach(combo, 4, sides[4].futureIds);

						for (int i4 = -1; i4 < src4.size(); i4++) {
							combo[4] = i4 < 0 ? -1 : src4.at(i4);

							if (check(joins, combo, builders, sides, exists) && exists) {
								return true;
							}
						}
					}
				}
			}
		}

		return false;
	}

	private boolean check(AJoin[] joins, long[] combo, LinksBuilder[] builders, JoinSide[] sides, boolean exists) {
		for (AJoin join : joins) {
			if (!has(join, combo, sides)) {
				return false;
			}
		}

		if (!exists) {
			for (AJoin join : joins) {
				builders[join.index].link(combo[join.from2], combo[join.to2]);
			}
		}

		return true;
	}

	private boolean has(AJoin join, long[] combo, JoinSide[] sides) {
		long from = combo[join.from2];
		long to = combo[join.to2];

		FutureIds fromIds = sides[join.from2].futureIds;
		FutureIds toIds = sides[join.to2].futureIds;

		Numbers filterFroms = fromIds.optional() ? null : fromIds.fetch();
		Numbers filterTos = toIds.optional() ? null : toIds.fetch();

		boolean lefty = to == -1 && from >= 0 && (filterTos == null || !join.rel.linksFrom(from).hasAny(filterTos));
		boolean righty = from == -1 && to >= 0 && (filterFroms == null || !join.rel.linksTo(to).hasAny(filterFroms));
		boolean linked = join.rel.hasLink(from, to);

		switch (join.mode) {
		case INNER:
			return hasInfo(linked, from, to, join.rel);
		case LEFT_OUTER:
			return hasInfo(U.xor(lefty, linked), from, to, join.rel);
		case RIGHT_OUTER:
			return hasInfo(U.xor(righty, linked), from, to, join.rel);
		case FULL_OUTER:
			return hasInfo(U.xor(U.xor(lefty, righty), linked), from, to, join.rel);
		}

		throw Errors.notExpected();
	}

	private boolean hasInfo(boolean rez, long from, long to, ReadOnlyRelation rel) {
		if (rez) {
			// System.out.println("  - " + rel.name() + " (" + from + " => " +
			// to + ")");
		}
		return rez;
	}

}
