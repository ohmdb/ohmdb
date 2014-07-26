package com.ohmdb.join;

/*
 * #%L
 * ohmdb-test
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

import org.testng.annotations.Test;

import com.ohmdb.abstracts.Numbers;
import com.ohmdb.abstracts.RWRelation;
import com.ohmdb.abstracts.ReadOnlyRelation;
import com.ohmdb.api.Links;
import com.ohmdb.dsl.join.DefaultJoinConfig;
import com.ohmdb.dsl.join.JoinAlternative;
import com.ohmdb.dsl.join.JoinBuilder;
import com.ohmdb.dsl.join.JoinConfig;
import com.ohmdb.dsl.join.JoinQuery;
import com.ohmdb.test.TestCommons;
import com.ohmdb.util.UTILS;

public class LinkMatcherTest extends TestCommons {

	private Numbers ids0;
	private Numbers ids1;
	private Numbers ids2;

	@Override
	protected void ready() {
		ids0 = ids0();
		ids1 = ids1();
		ids2 = ids2();
	}

	@Test
	public void shouldMatch2JoinedRelations() {
		ReadOnlyRelation rel1 = rel1(db);
		ReadOnlyRelation rel2 = rel2(db);

		JoinQuery query = JoinBuilder.make().join(0, rel1, 1, rel2, 2).build();

		System.out.println(query);

		DefaultJoinConfig params = (DefaultJoinConfig) jparam(query, nums(1, 2, 7), nums(30, 40, 90), nums(200, 333));
		System.out.println(params.joinsCount());

		// [[1: [30], 2: [30]] , [30: [333]]
		Links[] links = matcher().match(params);

		System.out.println("=== REZ === " + Arrays.toString(links));

		eq(links.length, 2);
		eqlinks(links[0], ln(1, 30), ln(2, 30));
		eqlinks(links[1], ln(30, 333));
	}

	@Test
	public void shouldMatch3JoinedRelations() {
		ReadOnlyRelation rel1 = rel1(db);
		ReadOnlyRelation rel2 = rel2(db);
		ReadOnlyRelation rel3 = rel3(db);

		JoinBuilder builder = JoinBuilder.make();
		builder.join(0, rel1, 1, rel2, 2);
		builder.join(0, rel3, 2);

		JoinQuery query = builder.build();

		JoinConfig params = jparam(query, nums(1, 2, 7), nums(20, 30, 40, 90), nums(200, 333, 2000, 3000));

		Links[] links = matcher().match(params);
		Links[] links2 = nlinks(links(ln(1, 20)), links(ln(20, 2000)), links(ln(1, 2000)));

		System.out.println("=== REZ === " + Arrays.toString(links));
		System.out.println("=== EXP === " + Arrays.toString(links2));

		isTrue(UTILS.equal(links, links2));
	}

	@Test
	public void shouldMatch3NulledJoinedRelations() {
		ReadOnlyRelation rel1 = rel1(db);
		ReadOnlyRelation rel2 = rel2(db);
		ReadOnlyRelation rel3 = rel3(db);

		JoinBuilder builder = JoinBuilder.make();
		builder.join(0, rel1, 2, rel2, 0);
		builder.join(1, rel3, 2);

		JoinQuery query = builder.build();

		JoinConfig params = jparam(query, ids0, nums(20, 30, 40, 90), ids2);

		Links[] links = matcher().match(params);
		Links[] links2 = nlinks(links(), links(), links());

		isTrue(UTILS.equal(links, links2));
	}

	@Test
	public void shouldMatchJoinedAlternativeRelations() {
		ReadOnlyRelation rel1 = rel1(db);
		ReadOnlyRelation rel2 = rel2(db);
		ReadOnlyRelation rel3 = rel3(db);

		JoinBuilder joiner = JoinBuilder.make();
		JoinAlternative or1 = joiner.or(0, rel1, 1, rel2, 2);
		JoinAlternative or2 = joiner.or(0, rel3, 2);
		joiner.alternatives(or1, or2);
		JoinQuery query = joiner.build();

		JoinConfig params = jparam(query, nums(1, 2, 7), nums(30, 40, 90), nums(200, 333));

		Links[] links = matcher().match(params);

		System.out.println("=== REZ === " + Arrays.toString(links));

		// FIXME complete
	}

	@Test
	public void shouldMatch2JoinedRelations2() {
		RWRelation rel0 = rel10x10("rel0", 0, 10, db, TBL1, TBL2);

		System.out.println(rel0.info());

		RWRelation rel1 = rel10x10("rel1", 10, 0, db, TBL2, TBL1);
		rel1.deleteFrom(18);
		UTILS.delink(rel1, 16, nums(3, 4, 6));
		UTILS.delink(rel1, 19, nums(3, 6, 7, 10));

		System.out.println(rel1.info());

		JoinQuery query = JoinBuilder.make().join(0, rel0, 1).join(1, rel1, 0).build();

		System.out.println(query);

		DefaultJoinConfig params = (DefaultJoinConfig) jparam(query, nums(0, 2, 3, 4, 5), nums(10, 12, 13, 16, 17));
		System.out.println(params.joinsCount());

		Links[] links = matcher().match(params);

		Links a = links(ln(10, 0, 2, 3, 4, 5), ln(12, 0, 2, 3, 4, 5), ln(13, 0, 2, 3, 4, 5), ln(16, 0, 2, 5),
				ln(17, 0, 2, 3, 4, 5));

		Links b = links(ln(0, 10, 12, 13, 16, 17), ln(2, 10, 12, 13, 16, 17), ln(3, 10, 12, 13, 17),
				ln(4, 10, 12, 13, 17), ln(5, 10, 12, 13, 16, 17));

		Links[] links2 = nlinks(b, a);

		System.out.println("=== REZ === " + Arrays.toString(links));
		System.out.println("=== EXP === " + Arrays.toString(links2));

		isTrue(UTILS.equal(links, links2));
	}

	@Test
	public void shouldMatchTwoWayRelations() {
		RWRelation rel1 = relation(db, TBL1, "rel1", TBL2);
		RWRelation rel2 = relation(db, TBL2, "rel2", TBL1);

		UTILS.link(rel1, 1, nums(10, 11, 99));
		UTILS.link(rel1, 2, nums(20, 21, 99));
		UTILS.link(rel1, 3, nums(99));

		UTILS.link(rel2, 10, nums(1, 3));
		UTILS.link(rel2, 20, nums(2, 3));

		UTILS.link(rel2, 11, nums(2, 3));
		UTILS.link(rel2, 21, nums(1, 3));

		System.out.println(rel1);
		System.out.println(rel2);

		JoinQuery query = JoinBuilder.make().join(0, rel1, 1, rel2, 0).build();

		System.out.println(query);

		DefaultJoinConfig params = (DefaultJoinConfig) jparam(query, nums(1, 2, 3, 4, 5), nums(10, 11, 20, 21, 99));
		System.out.println(params.joinsCount());

		Links[] links = matcher().match(params);

		// FIXME complete this
		// isTrue(UTILS.equal(links, links2));
	}

	@Test
	public void shouldMatchCommonTableRelations() {
		RWRelation rel1 = relation(db, TBL1, "rel1", TBL2);
		RWRelation rel2 = relation(db, TBL1, "rel2", TBL3);

		UTILS.link(rel1, 1, nums(11));
		UTILS.link(rel1, 2, nums(20));
		UTILS.link(rel1, 3, nums(99));

		UTILS.link(rel2, 1, nums(100));
		UTILS.link(rel2, 2, nums(201));
		UTILS.link(rel1, 3, nums(999));

		System.out.println(rel1);
		System.out.println(rel2);

		JoinQuery query = JoinBuilder.make().join(0, rel1, 1).join(0, rel2, 2).build();

		System.out.println(query);

		DefaultJoinConfig params = (DefaultJoinConfig) jparam(query, nums(1, 2, 3, 4, 5), nums(10, 20, 30),
				nums(100, 200, 300));
		System.out.println(params.joinsCount());

		Links[] links = matcher().match(params);
		Links[] links2 = nlinks(links(), links());

		System.out.println("=== REZ === " + Arrays.toString(links));
		System.out.println("=== EXP === " + Arrays.toString(links2));

		isTrue(UTILS.equal(links, links2));
	}

	@Test
	public void shouldIntersectMatched() {
		RWRelation rel1 = relation(db, TBL1, "rel1", TBL2);
		UTILS.link(rel1, 1, nums(10, 20));

		RWRelation rel2 = relation(db, TBL1, "rel2", TBL2);
		UTILS.link(rel2, 1, nums(20, 30));

		JoinQuery query = JoinBuilder.make().join(0, rel1, 1).join(0, rel2, 1).build();

		DefaultJoinConfig params = (DefaultJoinConfig) jparam(query, nums(1), nums(10, 20, 30));

		Links[] links = matcher().match(params);
		Links[] links2 = nlinks(links(ln(1, 20)), links(ln(1, 20)));

		System.out.println("=== REZ === " + Arrays.toString(links));
		System.out.println("=== EXP === " + Arrays.toString(links2));

		isTrue(UTILS.equal(links, links2));

	}

	@Test
	public void shouldMatch() {
		RWRelation rel1 = relation(db, TBL1, "rel1", TBL2);
		UTILS.link(rel1, 0, nums(10, 11, 13));
		UTILS.link(rel1, 1, nums(10, 11, 12, 13));
		UTILS.link(rel1, 2, nums(10, 11, 12, 13));

		RWRelation rel0 = relation(db, TBL1, "rel2", TBL2);
		UTILS.link(rel0, 0, nums(10, 11, 12, 13));
		UTILS.link(rel0, 1, nums(10, 13));
		UTILS.link(rel0, 2, nums(10, 11));

		JoinQuery query = JoinBuilder.make().join(0, rel1, 1).join(0, rel0, 1).build();

		DefaultJoinConfig params = (DefaultJoinConfig) jparam(query, nums(0, 1, 2, 5, 7), nums(12, 13, 16, 17));

		Links[] links = matcher().match(params);
		Links[] links2 = nlinks(links(ln(0, 13), ln(1, 13)), links(ln(0, 13), ln(1, 13)));

		System.out.println("=== REZ === " + Arrays.toString(links));
		System.out.println("=== EXP === " + Arrays.toString(links2));

		isTrue(UTILS.equal(links, links2));
	}

	@Test
	public void shouldMiniMatchWithoutIDs() {
		RWRelation rel1 = relation(db, TBL1, "rel1", TBL2);
		UTILS.link(rel1, 0, nums(10, 11, 13));
		UTILS.link(rel1, 1, nums(11, 12, 13));
		UTILS.link(rel1, 2, nums(10, 11, 12, 13));

		System.out.println(rel1.info());

		JoinQuery query = JoinBuilder.make().join(0, rel1, 1).build();

		DefaultJoinConfig params = (DefaultJoinConfig) jparam0to10(query, null, null);

		Links[] links = matcher().match(params);
		Links lns = links(ln(0, 10), ln(2, 10));
		Links[] links2 = nlinks(lns);

		System.out.println("=== REZ === " + Arrays.toString(links));
		System.out.println("=== EXP === " + Arrays.toString(links2));

		isTrue(UTILS.equal(links, links2));
	}

	@Test
	public void shouldMatchWithoutIDs() {
		RWRelation rel1 = relation(db, TBL1, "rel1", TBL2);
		UTILS.link(rel1, 0, nums(10, 11, 13));
		UTILS.link(rel1, 1, nums(11, 12, 13));
		UTILS.link(rel1, 2, nums(10, 11, 12, 13));

		RWRelation rel0 = relation(db, TBL1, "rel2", TBL2);
		UTILS.link(rel0, 0, nums(10, 11, 12, 13));
		UTILS.link(rel0, 1, nums(10, 13));

		JoinQuery query = JoinBuilder.make().join(0, rel1, 1).join(0, rel0, 1).build();

		DefaultJoinConfig params = (DefaultJoinConfig) jparam0to10(query, null, null);

		Links[] links = matcher().match(params);
		Links lns = links(ln(0, 10));
		Links[] links2 = nlinks(lns, lns);

		System.out.println("=== REZ === " + Arrays.toString(links));
		System.out.println("=== EXP === " + Arrays.toString(links2));

		isTrue(UTILS.equal(links, links2));
	}

	@Test
	public void shouldMatchWithoutIDsOnLeft() {
		RWRelation rel1 = relation(db, TBL1, "rel1", TBL2);
		UTILS.link(rel1, 0, nums(9, 10, 11, 13));
		UTILS.link(rel1, 1, nums(9, 10, 11, 12, 13));
		UTILS.link(rel1, 2, nums(9, 10, 11, 12, 13));

		RWRelation rel0 = relation(db, TBL1, "rel2", TBL2);
		UTILS.link(rel0, 0, nums(9, 10, 11, 12, 13));
		UTILS.link(rel0, 1, nums(13));
		UTILS.link(rel0, 2, nums(11));

		JoinQuery query = JoinBuilder.make().join(0, rel1, 1).join(0, rel0, 1).build();

		DefaultJoinConfig params = (DefaultJoinConfig) jparam0to10(query, null, nums(9));

		Links[] links = matcher().match(params);
		Links lns = links(ln(0, 9));
		Links[] links2 = nlinks(lns, lns);

		System.out.println("=== REZ === " + Arrays.toString(links));
		System.out.println("=== EXP === " + Arrays.toString(links2));

		isTrue(UTILS.equal(links, links2));
	}

	@Test
	public void shouldMatchWithoutIDsOnRight() {
		RWRelation rel1 = relation(db, TBL1, "rel1", TBL2);
		UTILS.link(rel1, 0, nums(9, 10, 11, 13));
		UTILS.link(rel1, 1, nums(9, 10));
		UTILS.link(rel1, 2, nums(9, 10, 11, 12, 13));

		RWRelation rel0 = relation(db, TBL1, "rel2", TBL2);
		UTILS.link(rel0, 0, nums(9, 10, 11, 12, 13));
		UTILS.link(rel0, 1, nums(9, 13));
		UTILS.link(rel0, 2, nums(11));

		JoinQuery query = JoinBuilder.make().join(0, rel1, 1).join(0, rel0, 1).build();

		DefaultJoinConfig params = (DefaultJoinConfig) jparam0to10(query, nums(1), null);

		Links[] links = matcher().match(params);
		Links lns = links(ln(1, 9));
		Links[] links2 = nlinks(lns, lns);

		System.out.println("=== REZ === " + Arrays.toString(links));
		System.out.println("=== EXP === " + Arrays.toString(links2));

		isTrue(UTILS.equal(links, links2));
	}

	@Test
	public void shouldDoSomeJoin() {
		RWRelation rel0 = relation(db, TBL1, "rel1", TBL2);
		UTILS.link(rel0, 1, nums(10));
		UTILS.link(rel0, 2, nums(10));

		RWRelation rel1 = relation(db, TBL1, "rel2", TBL2);
		UTILS.link(rel1, 10, nums(100, 200));

		JoinQuery query = JoinBuilder.make().join(0, rel0, 1, rel1, 2).build();

		JoinConfig params = jparam(query, nums(0, 1, 2), ids1, ids2);

		Links[] links = matcher().match(params);
		Links[] links2 = nlinks(links(ln(1, 10), ln(2, 10)), links(ln(10, 100, 200)));

		System.out.println("=== REZ === " + Arrays.toString(links));
		System.out.println("=== EXP === " + Arrays.toString(links2));

		isTrue(UTILS.equal(links, links2));
	}

}
