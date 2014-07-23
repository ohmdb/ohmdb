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

import com.ohmdb.abstracts.RWRelation;
import com.ohmdb.api.Ids;
import com.ohmdb.api.Join;
import com.ohmdb.api.JoinMode;
import com.ohmdb.api.JoinResult;
import com.ohmdb.api.Links;
import com.ohmdb.api.ParameterBinding;
import com.ohmdb.api.Relation;
import com.ohmdb.dsl.rel.impl.AbstractCommonRelation;
import com.ohmdb.join.DefaultJoinConfig;
import com.ohmdb.join.JoinBuilder;
import com.ohmdb.join.JoinConfig;
import com.ohmdb.join.JoinQuery;
import com.ohmdb.join.LinkMatcher;
import com.ohmdb.util.Errors;
import com.ohmdb.util.LIMIT;
import com.ohmdb.util.UTILS;

public class JoinImpl implements Join {

	private final Ids<?> from;
	private final Relation<?, ?> relation;
	private final Ids<?> to;
	private final JoinMode mode;
	private final JoinImpl prev;
	private final JoinConfig config;
	private final ParameterBinding<?> binding;
	private final LinkMatcher linkMatcher;

	public JoinImpl(LinkMatcher linkMatcher, Ids<?> from, Relation<?, ?> relation, Ids<?> to, JoinMode mode) {
		this.linkMatcher = linkMatcher;
		this.binding = null;
		this.from = from;
		this.relation = relation;
		this.to = to;
		this.mode = mode;
		this.prev = null;
		this.config = config();
	}

	public JoinImpl(LinkMatcher linkMatcher, Ids<?> from, Relation<?, ?> relation, Ids<?> to, JoinMode mode,
			JoinImpl chain) {
		this.linkMatcher = linkMatcher;
		this.binding = null;
		this.from = from;
		this.relation = relation;
		this.to = to;
		this.mode = mode;
		this.prev = chain;
		this.config = config();
	}

	public JoinImpl(LinkMatcher linkMatcher, ParameterBinding<?> binding, JoinImpl prev) {
		this.linkMatcher = linkMatcher;
		this.binding = binding;
		this.from = null;
		this.relation = null;
		this.to = null;
		this.mode = null;
		this.prev = prev;
		this.config = prev.config;
	}

	@Override
	public <FROM, TO> Join join(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to) {
		return new JoinImpl(linkMatcher, from, relation, to, JoinMode.INNER, this);
	}

	@Override
	public <FROM, TO> Join leftJoin(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to) {
		return new JoinImpl(linkMatcher, from, relation, to, JoinMode.LEFT_OUTER, this);
	}

	@Override
	public <FROM, TO> Join rightJoin(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to) {
		return new JoinImpl(linkMatcher, from, relation, to, JoinMode.RIGHT_OUTER, this);
	}

	@Override
	public <FROM, TO> Join fullJoin(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to) {
		return new JoinImpl(linkMatcher, from, relation, to, JoinMode.FULL_OUTER, this);
	}

	private JoinConfig config() {
		JoinBuilder builder = JoinBuilder.make();

		int joinsN = 0;
		int sidesN = 0;

		JoinImpl[] joins = new JoinImpl[LIMIT.MAX_JOINS];
		Ids<?>[] searches = new Ids<?>[LIMIT.MAX_SIDES];

		JoinImpl join = this;
		while (join != null) {
			if (joinsN >= LIMIT.MAX_JOINS) {
				throw Errors.rte("Maximum number of joins exceeded! Limit is: " + LIMIT.MAX_JOINS);
			}

			if (join.binding == null) {
				joins[joinsN++] = join;
			} else {
				// FIXME bindings in join
				throw Errors.rte("Bindings in joins are not implemented yet!");
			}

			join = join.prev;

		}

		for (int i = joinsN - 1; i >= 0; i--) {
			join = joins[i];

			Ids<?> from_ = join.from;
			int fromIndex = find(searches, from_, sidesN);
			if (fromIndex < 0) {
				if (sidesN >= LIMIT.MAX_SIDES) {
					throw Errors.rte("Maximum number of join elements exceeded! Limit is: " + LIMIT.MAX_SIDES);
				}
				fromIndex = sidesN++;
				searches[fromIndex] = from_;
			}

			Ids<?> to_ = join.to;
			int toIndex = find(searches, to_, sidesN);
			if (toIndex < 0) {
				if (sidesN >= LIMIT.MAX_SIDES) {
					throw Errors.rte("Maximum number of join elements exceeded! Limit is: " + LIMIT.MAX_SIDES);
				}
				toIndex = sidesN++;
				searches[toIndex] = to_;
			}

			RWRelation rel = ((AbstractCommonRelation<?, ?>) join.relation).rel;
			builder.joinMode(fromIndex, rel, toIndex, join.mode);
		}

		Ids<?>[] searches2 = new Ids<?>[sidesN];
		System.arraycopy(searches, 0, searches2, 0, sidesN);

		JoinQuery query = builder.build();
		JoinConfig config = new DefaultJoinConfig(UTILS.futureIds(searches2), query.joins());

		return config;
	}

	private int find(Ids<?>[] searches, Ids<?> search, int count) {
		for (int i = 0; i < count; i++) {
			if (searches[i] == search) {
				return i;
			}
		}

		return -1;
	}

	@Override
	public Join bind(ParameterBinding<?> binding) {
		return new JoinImpl(linkMatcher, binding, this);
	}

	private Links[] match() {
		return linkMatcher.match(config);
	}

	@Override
	public boolean exists() {
		Links[] links = match();

		for (Links ln : links) {
			if (ln.size() > 0) {
				return true;
			}
		}

		return false;
	}

	@Override
	public JoinResult all() {
		return new JoinResultImpl(match());
	}

}
