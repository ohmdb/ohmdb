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

import com.ohmdb.test.TestCommons;

public class RelationMatchTest extends TestCommons {

//	@Test
//	public void shouldMatchInnerRelation1() {
//		ReadOnlyRelation rel1 = RelFixture.rel1(db);
//		// [1: [20, 30], 7: [88]]
//		Links p0 = ln(rel1.join(nums(1, 7), null, true, JoinMode.INNER));
//
//		eqlinks(p0, ln(1, 20, 30), ln(7, 88));
//		eqnums(p0.left(), 1, 7);
//		eqnums(p0.right(), 20, 30, 88);
//
//	}
//
//	@Test
//	public void shouldMatchInnerRelation2() {
//		ReadOnlyRelation rel1 = RelFixture.rel1(db);
//		// [1: [20, 30], 7: [88]]
//		Links p1 = ln(rel1.join(nums(1, 7, 13), null, true, JoinMode.INNER));
//
//		eqlinks(p1, ln(1, 20, 30), ln(7, 88));
//		eqnums(p1.left(), 1, 7);
//		eqnums(p1.right(), 20, 30, 88);
//
//	}
//
//	@Test
//	public void shouldMatchInnerRelation3() {
//		ReadOnlyRelation rel1 = RelFixture.rel1(db);
//		// [1: [20, 30], 2: [30, 40]]
//		Links p2 = ln(rel1.join(null, nums(20, 30, 40, 999), true, JoinMode.INNER));
//
//		eqlinks(p2, ln(1, 20, 30), ln(2, 30, 40));
//		eqnums(p2.left(), 1, 2);
//		eqnums(p2.right(), 20, 30, 40);
//	}
//
//	@Test
//	public void shouldMatchInnerRelation4() {
//		ReadOnlyRelation rel1 = RelFixture.rel1(db);
//		// [1: [20], 7: [88]]
//		Links p3 = ln(rel1.join(nums(1, 7), nums(20, 30, 40, 88), true, JoinMode.INNER));
//
//		System.out.println(p3);
//		eqlinks(p3, ln(1, 20, 30), ln(7, 88));
//		eqnums(p3.left(), 1, 7);
//		eqnums(p3.right(), 20, 30, 88);
//	}
//
//	@Test
//	public void shouldMatchInnerRelation5() {
//		ReadOnlyRelation rel1 = RelFixture.rel1(db);
//		// [1: [30], 7: [88]]
//		Links p4 = ln(rel1.join(nums(1, 7, 11, 13), nums(30, 88, 999), true, JoinMode.INNER));
//
//		eqlinks(p4, ln(1, 30), ln(7, 88));
//		eqnums(p4.left(), 1, 7);
//		eqnums(p4.right(), 30, 88);
//	}
//
//	@Test
//	public void shouldMatchOuterRelation() {
//		ReadOnlyRelation rel1 = RelFixture.rel1(db);
//		// [1: [20, 30], 13: []]
//		Links p5 = ln(rel1.join(nums(1, 13), nums(20, 30), true, JoinMode.LEFT_OUTER));
//
//		eqlinks(p5, ln(1, 20, 30), ln(13));
//		eqnums(p5.left(), 1, 13);
//		eqnums(p5.right(), 20, 30);
//	}
//
//	@Test
//	public void shouldMatchOuterRelationWithoutTo() {
//		ReadOnlyRelation rel1 = RelFixture.rel1(db);
//		// [1: [20, 30], 13: []]
//		Links p5 = ln(rel1.join(nums(1, 13), null, true, JoinMode.LEFT_OUTER));
//
//		eqlinks(p5, ln(1, 20, 30), ln(13));
//		eqnums(p5.left(), 1, 13);
//		eqnums(p5.right(), 20, 30);
//	}
//
//	@Test
//	public void shouldMatchOuterRelationWithoutFrom() {
//		ReadOnlyRelation rel1 = RelFixture.rel1(db);
//		// [1: [20, 30], 2: [30, 40]]
//		Links p6 = ln(rel1.join(null, nums(20, 30, 40, 999), true, JoinMode.LEFT_OUTER));
//
//		eqlinks(p6, ln(1, 20, 30), ln(2, 30, 40), ln(7));
//		eqnums(p6.left(), 1, 2, 7);
//		eqnums(p6.right(), 20, 30, 40);
//	}
//
//	@Test
//	public void shouldSupportLeftToRightInnerJoin() {
//		ReadOnlyRelation rel1 = RelFixture.rel1(db);
//
//		// []
//		Links p = ln(rel1.join(nums(1, 2), nums(777, 888, 999), true, JoinMode.INNER));
//
//		eqlinks(p);
//		eqnums(p.left());
//		eqnums(p.right());
//	}
//
//	@Test
//	public void shouldSupportRightToLeftInnerJoin() {
//		ReadOnlyRelation rel1 = RelFixture.rel1(db);
//
//		// []
//		Links p = ln(rel1.join(nums(1, 2), nums(999), true, JoinMode.INNER));
//
//		eqlinks(p);
//		eqnums(p.left());
//		eqnums(p.right());
//	}
//
//	@Test
//	public void shouldSupportLeftToRightOuterJoin() {
//		ReadOnlyRelation rel1 = RelFixture.rel1(db);
//
//		// [1: [], 2: []]
//		Links p = ln(rel1.join(nums(1, 2), nums(777, 888, 999), true, JoinMode.LEFT_OUTER));
//
//		eqlinks(p, ln(1), ln(2));
//		eqnums(p.left(), 1, 2);
//		eqnums(p.right());
//	}
//
//	@Test
//	public void shouldSupportRightToLeftOuterJoin() {
//		ReadOnlyRelation rel1 = RelFixture.rel1(db);
//
//		// [1: [], 2: [40], 7:[]]
//		Links p = ln(rel1.join(nums(1, 2, 7), nums(40, 99), true, JoinMode.RIGHT_OUTER));
//
//		System.out.println(p);
//		
//		eqlinks(p, ln(1), ln(2, 40), ln(7));
//		eqnums(p.left(), 1, 2, 7);
//		eqnums(p.right(), 40);
//	}

}
