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

import com.ohmdb.util.MemUtils;

public class Numbers {

	private final long[] arr;

	public Numbers(long[] src, int start, int length) {
		this.arr = new long[length];
		System.arraycopy(src, start, arr, 0, length);
	}

	public Numbers(long[] nums) {
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
		for (long num : nums.arr) {
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
		return new Numbers(MemUtils.insert(arr, num));
	}

	public Numbers without(long num) {
		return new Numbers(MemUtils.removeFirstInSorted(arr, num));
	}
	
}
