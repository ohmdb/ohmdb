package com.ohmdb.test;

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

import com.ohmdb.abstracts.Any;
import com.ohmdb.numbers.Nums;

public class TestAny implements Any<Object> {

	private final int from;
	private final int to;

	public TestAny(int from, int to) {
		this.from = from;
		this.to = to;
	}

	@Override
	public long[] ids() {
		return null;
	}

	@Override
	public int size() {
		return to - from + 1;
	}

	@Override
	public long[] all() {
		return Nums.arrFromTo(from, to);
	}

}
