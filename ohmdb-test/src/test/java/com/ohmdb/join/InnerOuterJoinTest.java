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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.ohmdb.abstracts.RWRelation;
import com.ohmdb.api.Links;
import com.ohmdb.numbers.Numbers;
import com.ohmdb.test.TestCommons;
import com.ohmdb.util.UTILS;

public class InnerOuterJoinTest extends TestCommons {

	private RWRelation relA;
	private RWRelation relB;

	private RWRelation rel1;
	private RWRelation rel2;

	private Numbers ids1;
	private Numbers ids2;
	private Numbers allIds;

	@BeforeMethod
	public void init() {
		System.out.println("- INIT BEFORE TEST!");

		relA = relation(db, TBL1, "relA", TBL2);
		UTILS.link(relA, 0, nums(7, 8));
		UTILS.link(relA, 2, nums(9));

		relB = relation(db, TBL1, "relB", TBL2);
		UTILS.link(relB, 1, nums(5, 6));
		UTILS.link(relB, 3, nums(7));

		rel1 = relation(db, TBL1, "rel1", TBL2);
		UTILS.link(rel1, 0, nums(10, 30));
		UTILS.link(rel1, 2, nums(20));

		rel2 = relation(db, TBL2, "rel2", TBL3);
		UTILS.link(rel2, 1, nums(100));
		UTILS.link(rel2, 2, nums(200));

		allIds = nums(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15, 20, 30, 40, 50, 100, 200, 300);
		ids1 = nums(10, 20, 30);
		ids2 = nums(100, 200);

		System.out.println("- RELATIONS ARE INITIALIZED.");
		System.out.println(rel1.info());
	}

	@Test
	public void shouldJoinLeftOuter1() {
		System.out.println(rel1.info());
		JoinQuery query = JoinBuilder.make().leftJoin(0, rel1, 1).build();
		JoinConfig config = jparam(query, nums(0, 1, 2), ids1);

		Links[] expect = nlinks(links(ln(0, 10, 30), noln(1), ln(2, 20)));

		checkJoin(config, expect);
	}

	@Test
	public void shouldJoinLeftAndLeft() {
		System.out.println(rel1.info());
		System.out.println(rel2.info());
		JoinQuery query = JoinBuilder.make().leftJoin(0, rel1, 1).leftJoin(0, rel2, 2).build();
		JoinConfig config = jparam(query, nums(0, 1, 2), ids1, ids2);

		Links[] expect = nlinks(links(ln(0, 10, 30), noln(1), ln(2, 20)), links(noln(0), ln(1, 100), ln(2, 200)));

		checkJoin(config, expect);
	}

	@Test
	public void shouldJoinLeftAndInner() {
		System.out.println(rel1.info());
		System.out.println(rel2.info());
		JoinQuery query = JoinBuilder.make().leftJoin(0, rel1, 1).join(0, rel2, 2).build();
		JoinConfig config = jparam(query, nums(0, 1, 2), ids1, ids2);

		Links[] expect = nlinks(links(noln(1), ln(2, 20)), links(ln(1, 100), ln(2, 200)));

		checkJoin(config, expect);
	}

	@Test
	public void shouldJoinInnerAndLeft() {
		System.out.println(rel1.info());
		System.out.println(rel2.info());
		JoinQuery query = JoinBuilder.make().join(0, rel1, 1).leftJoin(0, rel2, 2).build();
		JoinConfig config = jparam(query, nums(0, 1, 2), ids1, ids2);

		Links[] expect = nlinks(links(ln(0, 10, 30), ln(2, 20)), links(noln(0), ln(2, 200)));

		checkJoin(config, expect);
	}

	@Test
	public void shouldJoinInnerAndInner() {
		System.out.println(rel1.info());
		System.out.println(rel2.info());
		JoinQuery query = JoinBuilder.make().join(0, rel1, 1).join(0, rel2, 2).build();
		JoinConfig config = jparam(query, nums(0, 1, 2), allIds, allIds);

		Links[] expect = nlinks(links(ln(2, 20)), links(ln(2, 200)));

		checkJoin(config, expect);
	}

	@Test
	public void shouldJoinRightOuter1() {
		System.out.println(rel1.info());
		JoinQuery query = JoinBuilder.make().rightJoin(0, rel1, 1).build();
		JoinConfig config = jparam(query, nums(0, 1, 2), nums(10, 20, 30, 40));

		Links[] expect = nlinks(links(ln(-1, 40), ln(0, 10, 30), ln(2, 20)));

		checkJoin(config, expect);
	}

}
