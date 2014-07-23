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

import com.ohmdb.abstracts.ReadOnlyRelation;
import com.ohmdb.api.JoinMode;

public class JoinBuilder {

	private final List<JoinSegment> segments = new ArrayList<JoinSegment>();

	private int counter = 0;

	private AJoin trelt(int from, ReadOnlyRelation rel, int to, JoinMode mode) {
		return new AJoin(counter++, from, rel, to, mode);
	}

	private AJoin trelt(int from, ReadOnlyRelation rel, int to) {
		return new AJoin(counter++, from, rel, to, JoinMode.INNER);
	}

	public JoinBuilder joinMode(int from, ReadOnlyRelation rel, int to, JoinMode mode) {
		segments.add(new JoinSegment(or(from, rel, to, mode)));
		return this;
	}

	public JoinBuilder join(int from, ReadOnlyRelation rel, int to) {
		return joinMode(from, rel, to, JoinMode.INNER);
	}

	public JoinBuilder leftJoin(int from, ReadOnlyRelation rel, int to) {
		return joinMode(from, rel, to, JoinMode.LEFT_OUTER);
	}

	public JoinBuilder rightJoin(int from, ReadOnlyRelation rel, int to) {
		return joinMode(from, rel, to, JoinMode.RIGHT_OUTER);
	}

	public JoinBuilder fullJoin(int from, ReadOnlyRelation rel, int to) {
		return joinMode(from, rel, to, JoinMode.FULL_OUTER);
	}

	public JoinBuilder join(int from, ReadOnlyRelation rel1, int mid, ReadOnlyRelation rel2, int to) {
		segments.add(new JoinSegment(or(from, rel1, mid)));
		segments.add(new JoinSegment(or(mid, rel2, to)));
		return this;
	}

	public JoinBuilder join(int a, ReadOnlyRelation rel1, int b, ReadOnlyRelation rel2, int c, ReadOnlyRelation rel3,
			int d) {
		segments.add(new JoinSegment(or(a, rel1, b)));
		segments.add(new JoinSegment(or(b, rel2, c)));
		segments.add(new JoinSegment(or(c, rel3, d)));
		return this;
	}

	public JoinAlternative or(int from, ReadOnlyRelation rel, int to, JoinMode mode) {
		return new JoinAlternative(trelt(from, rel, to, mode));
	}

	public JoinAlternative or(int from, ReadOnlyRelation rel, int to) {
		return or(from, rel, to, JoinMode.INNER);
	}

	public JoinAlternative or(int from, ReadOnlyRelation rel1, int mid, ReadOnlyRelation rel2, int to) {
		return new JoinAlternative(trelt(from, rel1, mid), trelt(mid, rel2, to));
	}

	public JoinAlternative or(int a, ReadOnlyRelation rel1, int b, ReadOnlyRelation rel2, int c, ReadOnlyRelation rel3,
			int d) {
		return new JoinAlternative(trelt(a, rel1, b), trelt(b, rel2, c), trelt(c, rel3, d));
	}

	public JoinBuilder alternatives(JoinAlternative... ors) {
		segments.add(new JoinSegment(ors));
		return this;
	}

	public JoinQuery build() {
		return new JoinQuery(segments.toArray(new JoinSegment[segments.size()]));
	}

	public static JoinBuilder make() {
		return new JoinBuilder();
	}

}
