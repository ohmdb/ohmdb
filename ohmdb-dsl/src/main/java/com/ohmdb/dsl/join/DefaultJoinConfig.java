package com.ohmdb.dsl.join;

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

import com.ohmdb.abstracts.FutureIds;
import com.ohmdb.util.Check;
import com.ohmdb.util.U;

public class DefaultJoinConfig implements JoinConfig {

	private final AJoin[] joins;
	private final FutureIds[] futureIds;
	private JoinSide[] sides;

	public DefaultJoinConfig(FutureIds[] futureIds, AJoin[] joins) {
		this.futureIds = futureIds;
		this.joins = joins;
	}

	@Override
	public int sidesCount() {
		return sides.length;
	}

	@Override
	public String toString() {
		return "<JOIN " + Arrays.toString(sides) + " AS " + U.join(joins, ", ") + ">";
	}

	@Override
	public int joinsCount() {
		return joins.length;
	}

	@Override
	public AJoin[] getJoins() {
		return joins;
	}

	@Override
	public JoinSide[] sides() {
		Check.state(sides != null, "Join is not initialized!");
		return sides;
	}

	@Override
	public void initialize(JoinInitializer initializer) {
		this.sides = initializer.optimize(joins, futureIds);
	}

}
