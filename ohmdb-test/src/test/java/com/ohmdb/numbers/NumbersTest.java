package com.ohmdb.numbers;

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

import org.testng.annotations.Test;

import com.ohmdb.abstracts.Numbers;
import com.ohmdb.test.TestCommons;

public class NumbersTest extends TestCommons {

	@Test
	public void shouldConstructNumbers() {
		Numbers nums = nums(1, 3, 5);
		eq(nums.size(), 3);
		eqnums(nums, 1, 3, 5);
	}

	@Test
	public void shouldSupportUnion() {
		Numbers union = Nums.union(nums(10, 30, 50, 70, 90), nums(20, 30, 40, 50, 100));
		eqnums(union, 10, 20, 30, 40, 50, 70, 90, 100);

		Numbers union2 = Nums.unionAll(nums(15, 30, 55, 70, 95), union, nums(1, 50, 100));
		eqnums(union2, 1, 10, 15, 20, 30, 40, 50, 55, 70, 90, 95, 100);

		Numbers union3 = Nums.unionAll(nums(15, 30, 55, 70, 95), union, nums(1, 50, 100));
		eqnums(union3, 1, 10, 15, 20, 30, 40, 50, 55, 70, 90, 95, 100);
	}

	@Test
	public void shouldSupportUnionWithEmptyNums() {
		Numbers union1 = Nums.union(nums(1), nums());
		eqnums(union1, 1);

		Numbers union2 = Nums.unionAll(nums(), nums(1), nums());
		eqnums(union2, 1);
	}

	@Test
	public void shouldSupportEmptyUnion() {
		Numbers union1 = Nums.unionAll(nums());
		eqnums(union1);

		Numbers union2 = Nums.union(nums(), nums());
		eqnums(union2);

		Numbers union3 = Nums.unionAll(nums(), nums(), nums());
		eqnums(union3);
	}

	@Test
	public void shouldSupportIntersection() {
		Numbers inter = Nums.intersect(nums(1, 3, 5, 6, 7), nums(2, 3, 4, 6, 8));
		eqnums(inter, 3, 6);
	}

	@Test
	public void shouldSupportMultiIntersection2() {
		Numbers[] numms = { nums(1, 3, 5, 6, 7), nums(2, 3, 4, 6, 8) };
		Numbers inter = Nums.intersectAll(numms);
		eqnums(inter, 3, 6);
	}

	@Test
	public void shouldSupportMultiIntersection3() {
		Numbers[] numms = { nums(1, 3, 5, 6, 7), nums(2, 3, 4, 6, 8), nums(2, 5, 6, 8) };
		Numbers inter = Nums.intersectAll(numms);
		eqnums(inter, 6);
	}

	@Test
	public void shouldSupportMultiIntersection4() {
		Numbers[] numms = { nums(1, 3, 5, 6, 7), nums(2, 3, 4, 6, 8), nums(2, 5, 6, 8), nums(1, 2, 5, 6, 8) };
		Numbers inter = Nums.intersectAll(numms);
		eqnums(inter, 6);
	}

	@Test
	public void shouldSupportEmptyIntersection() {
		Numbers inter = Nums.intersect(nums(1, 3, 5), nums(2, 4, 6));
		eqnums(inter);
	}

}
