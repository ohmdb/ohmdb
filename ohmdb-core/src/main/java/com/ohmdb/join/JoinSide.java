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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.ohmdb.join.futureid.FutureIds;

public class JoinSide {

	public final int index;
	public int position;
	public FutureIds futureIds;

	public final Set<AJoin> froms = new HashSet<AJoin>();
	public final Set<AJoin> tos = new HashSet<AJoin>();
	public final Set<AJoin> joins = new HashSet<AJoin>();

	public final Set<JoinSide> related = new HashSet<JoinSide>();
	public final Set<JoinSide> before = new HashSet<JoinSide>();

	public JoinBridge bridge;

	public JoinSide(int index) {
		this.index = index;
		this.position = index;
	}

	public String details() {
		return "JoinSide [index=" + index + ", position=" + position + ", ids=" + futureIds + ", froms=" + froms
				+ ", tos=" + tos + ", joins=" + joins + ", related=" + info(related) + ", before=" + info(before)
				+ ", bridge=" + bridge + "]";
	}

	@Override
	public String toString() {
		return String.valueOf(futureIds);
	}

	private String info(Collection<JoinSide> sides) {
		String s = "";

		for (JoinSide side : sides) {
			s += " " + side.index;
		}

		return s;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + index;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		JoinSide other = (JoinSide) obj;
		if (index != other.index)
			return false;
		return true;
	}

}
