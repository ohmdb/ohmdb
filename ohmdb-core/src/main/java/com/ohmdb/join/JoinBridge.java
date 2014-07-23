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

import java.util.Set;

import com.ohmdb.api.JoinMode;
import com.ohmdb.join.futureid.FutureIds;
import com.ohmdb.numbers.Numbers;
import com.ohmdb.numbers.Nums;
import com.ohmdb.util.Check;

public class JoinBridge {

	// from rels - inner or right join
	private final Set<JoinBridgeRel> fromRels;

	// to rels - inner or left join
	private final Set<JoinBridgeRel> toRels;

	public JoinBridge(Set<JoinBridgeRel> fromRels, Set<JoinBridgeRel> toRels) {
		this.fromRels = fromRels;
		this.toRels = toRels;

		for (JoinBridgeRel bridgeRel : fromRels) {
			Check.arg(bridgeRel.mode == JoinMode.INNER || bridgeRel.mode == JoinMode.RIGHT_OUTER, "wrong join type!");
		}

		for (JoinBridgeRel bridgeRel : toRels) {
			Check.arg(bridgeRel.mode == JoinMode.INNER || bridgeRel.mode == JoinMode.LEFT_OUTER, "wrong join type!");
		}
	}

	public Numbers reach(long[] combo, int level, FutureIds futureIds) {
		Numbers src = futureIds.optional() ? null : futureIds.fetch();

		for (JoinBridgeRel bridgeRel : fromRels) {
			long id = combo[bridgeRel.index];
			Numbers filter = bridgeRel.rel.linksTo(id);
			src = inter(src, filter);
		}

		for (JoinBridgeRel bridgeRel : toRels) {
			long id = combo[bridgeRel.index];
			Numbers filter = bridgeRel.rel.linksFrom(id);
			src = inter(src, filter);
		}

		if (src == null) {
			src = futureIds.fetch();
		}

		return src;
	}

	private Numbers inter(Numbers src, Numbers filter) {
		return src != null ? Nums.intersect(src, filter) : filter;
	}

	@Override
	public String toString() {
		return "JoinBridge [fromRels=" + fromRels + ", toRels=" + toRels + "]";
	}

}
