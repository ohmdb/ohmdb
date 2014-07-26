package com.ohmdb.perf;

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

import java.util.TreeSet;

import org.testng.annotations.Test;

import com.ohmdb.abstracts.Numbers;
import com.ohmdb.numbers.Nums;
import com.ohmdb.test.TestCommons;
import com.ohmdb.util.Measure;

public class MergerPerfTest extends TestCommons {

	@Test
	public void unionAndIntersectionPerfSmall() {
		Numbers[] all = { nums(1, 2, 3, 4, 5, 6, 9, 12), Nums.from(1, 2, 4, 6, 7, 10), nums(2, 3, 5, 6, 7, 8, 9, 10) };

		int count = 1000000;

		Measure.start(count);

		for (int i = 0; i < count; i++) {
			Nums.intersectAll(all);
			Nums.unionAll(all);
		}

		Measure.finish("small union + small intersect");
	}

	@Test
	public void unionAndIntersectionPerfBig() {
		TreeSet<Long> a = new TreeSet<Long>();
		TreeSet<Long> b = new TreeSet<Long>();
		TreeSet<Long> c = new TreeSet<Long>();

		for (int i = 0; i < 1000; i++) {
			a.add((long) rnd(1000));
			b.add((long) rnd(1000));
			c.add((long) rnd(1000));
		}

		int count = 1000;

		Numbers[] big = { Nums.from(a), Nums.from(b), Nums.from(c) };

		Measure.start(count);

		for (int n = 0; n < count; n++) {
			Nums.intersectAll(big);
			Nums.unionAll(big);
		}

		Measure.finish("big union + big intersect");
	}

}
