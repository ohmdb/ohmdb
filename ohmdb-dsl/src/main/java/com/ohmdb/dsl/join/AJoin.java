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

import com.ohmdb.abstracts.ReadOnlyRelation;
import com.ohmdb.api.JoinMode;

public class AJoin {

	public final int index;

	public final int from;

	public final int to;

	public int from2;

	public int to2;

	public final ReadOnlyRelation rel;

	public final boolean inverse;

	public final JoinMode mode;

	public AJoin(int index, int from, ReadOnlyRelation rel, int to, JoinMode mode) {
		this.from = from;
		this.to = to;
		this.from2 = from;
		this.to2 = to;
		this.rel = rel;
		this.index = index;
		this.mode = mode;
		this.inverse = false;
	}

	public String details() {
		return "[#" + index + " " + mode + " " + rel.name() + " (" + from + "/" + from2 + "->" + to + "/" + to2 + ")";
	}

	@Override
	public String toString() {
		return mode + "(#" + from + " " + rel.name() + " #" + to + ")";
	}

}
