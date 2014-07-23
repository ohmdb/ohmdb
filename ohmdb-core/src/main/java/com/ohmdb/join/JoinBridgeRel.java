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

import com.ohmdb.abstracts.ReadOnlyRelation;
import com.ohmdb.api.JoinMode;

public class JoinBridgeRel {

	public final ReadOnlyRelation rel;

	public final int index;

	public final JoinMode mode;

	public JoinBridgeRel(ReadOnlyRelation rel, int index, JoinMode mode) {
		this.rel = rel;
		this.index = index;
		this.mode = mode;
	}

	@Override
	public String toString() {
		return "JoinBridgeRel [rel=" + rel + ", index=" + index + ", mode=" + mode + "]";
	}

}
