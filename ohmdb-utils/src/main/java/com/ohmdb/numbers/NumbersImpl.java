package com.ohmdb.numbers;

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

import java.util.Arrays;

import com.ohmdb.abstracts.Numbers;

public class NumbersImpl implements Numbers {

	private final long[] arr;

	public NumbersImpl(long[] src, int start, int length) {
		this.arr = new long[length];
		System.arraycopy(src, start, arr, 0, length);
	}

	public NumbersImpl(long[] nums) {
		this(nums, 0, nums.length);
	}

	public int size() {
		return arr.length;
	}

	public long[] toArray() {
		return arr;
	}

	public long at(int index) {
		return arr[index];
	}

	public boolean hasAny(Numbers nums) {
		// TODO use visitor to decouple from specific impl
		for (long num : ((NumbersImpl) nums).arr) {
			if (contains(num)) {
				return true;
			}
		}
		return false;
	}

	public boolean contains(long num) {
		return Arrays.binarySearch(arr, num) >= 0;
	}

	@Override
	public String toString() {
		return Arrays.toString(arr);
	}

	public Numbers with(long num) {
		return new NumbersImpl(insert(arr, num));
	}

	public Numbers without(long num) {
		return new NumbersImpl(removeFirstInSorted(arr, num));
	}

	public static long[] insert(long[] arr, long num) {
		int index = Arrays.binarySearch(arr, num);

		if (index < 0) {
			int pos = -index - 1;
			return insertAt(arr, num, pos);
		} else {
			// throw Errors.rte("It's already there!");
			return arr;
		}
	}

	public static long[] insertAt(long[] arr, long num, int pos) {
		long[] bigger = new long[arr.length + 1];

		// copy left
		System.arraycopy(arr, 0, bigger, 0, pos);

		// insert
		bigger[pos] = num;

		// copy right
		System.arraycopy(arr, pos, bigger, pos + 1, arr.length - pos);

		return bigger;
	}

	public static long[] removeFirstInSorted(long[] arr, long value) {
		int index = Arrays.binarySearch(arr, value);

		if (index < 0) {
			return arr;
		}

		long[] smaller = new long[arr.length - 1];

		// copy left
		System.arraycopy(arr, 0, smaller, 0, index);

		// copy right
		System.arraycopy(arr, index + 1, smaller, index, arr.length - index - 1);

		return smaller;
	}

}
