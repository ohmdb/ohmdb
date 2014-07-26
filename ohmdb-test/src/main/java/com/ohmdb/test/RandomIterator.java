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

import java.util.Iterator;
import java.util.Random;

public class RandomIterator implements Iterator<Long> {

	private final Random rnd = new Random();

	private final int count;
	private final int max;

	private int n = 0;

	public RandomIterator(int count, int max) {
		this.count = count;
		this.max = max;
	}

	@Override
	public boolean hasNext() {
		return ++n <= count;
	}

	@Override
	public Long next() {
		return (long) rnd.nextInt(max);
	}

	@Override
	public void remove() {
	}

}
