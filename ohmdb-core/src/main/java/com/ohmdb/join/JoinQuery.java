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

import java.util.ArrayList;
import java.util.List;

import com.ohmdb.util.Check;
import com.ohmdb.util.U;

public class JoinQuery {

	private JoinSegment[] segments;

	public JoinQuery(JoinSegment[] segments) {
		this.segments = segments;
	}

	@Override
	public String toString() {
		return "JoinQuery [segments=" + U.textln(segments) + "]";
	}

	public AJoin[] joins() {
		List<AJoin> joinsLst = new ArrayList<AJoin>();

		for (JoinSegment segment : segments) {
			for (JoinAlternative alt : segment.alternatives) {
				for (AJoin trt : alt.rels) {
					Check.state(trt.from != trt.to);
					joinsLst.add(trt);
				}
			}
		}
		return joinsLst.toArray(new AJoin[joinsLst.size()]);
	}

}
