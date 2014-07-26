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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

import com.ohmdb.abstracts.Numbers;
import com.ohmdb.util.Check;
import com.ohmdb.util.Errors;

public class Nums {

	private static final Numbers NONE = from();

	public static Numbers none() {
		return NONE;
	}

	public static Numbers from(long... nums) {
		return new NumbersImpl(nums);
	}

	public static Numbers from(long[] nums, int start, int length) {
		return new NumbersImpl(nums, start, length);
	}

	public static Numbers from(int... nums) {

		long[] nums2 = new long[nums.length];

		for (int i = 0; i < nums.length; i++) {
			nums2[i] = nums[i];
		}

		return from(nums2);
	}

	public static Numbers from(Object[] nums) {

		long[] nums2 = new long[nums.length];

		for (int i = 0; i < nums.length; i++) {
			nums2[i] = ((Number) nums[i]).longValue();
		}

		return from(nums2);
	}

	public static long[] arrFrom(SortedSet<? extends Number> numSet) {
		long[] nums = new long[numSet.size()];

		int i = 0;
		for (Number num : numSet) {
			nums[i++] = num.longValue();
		}

		return nums;
	}

	public static Numbers from(SortedSet<? extends Number> numSet) {
		long[] nums = new long[numSet.size()];

		int i = 0;
		for (Number num : numSet) {
			nums[i++] = num.longValue();
		}

		return from(nums);
	}

	public static Numbers fromArray(Object array) {
		if (array instanceof int[]) {
			return from((int[]) array);
		} else if (array instanceof long[]) {
			return from((long[]) array);
		} else if (array instanceof Object[]) {
			return from((Object[]) array);
		} else if (array == null) {
			return none();
		} else {
			throw Errors.rte("Wrong array type: " + array);
		}
	}

	public static int rnd(int n) {
		return (int) (Math.random() * n);
	}

	public static int rnd(int min, int max) {
		return min + rnd(max - min + 1);
	}

	private static boolean contains(int[] nums, int count, int val) {
		for (int i = 0; i < count; i++) {
			if (nums[i] == val) {
				return true;
			}
		}
		return false;
	}

	public static Numbers random(int minCount, int maxCount, int minVal, int maxVal) {
		int possible = maxVal - minVal + 1;
		Check.state(maxCount <= possible);

		int[] nums = new int[rnd(minCount, maxCount)];

		int count = 0;
		int step = 0;
		for (int i = 0; i < nums.length; i += step) {
			int val = rnd(minVal, maxVal);
			if (!contains(nums, count, val)) {
				nums[count++] = val;
				step = 1;
			} else {
				step = 0;
			}
		}

		Arrays.sort(nums);

		return from(nums);
	}

	public static Numbers[] randoms(int count, int minCount, int maxCount, int minVal, int maxVal) {
		Numbers[] nums = new Numbers[count];

		for (int i = 0; i < nums.length; i++) {
			nums[i] = random(minCount, maxCount, minVal, maxVal);
		}

		return nums;
	}

	public static Numbers fromTo(int from, int to) {
		int[] nums = new int[to - from + 1];

		for (int i = 0; i < nums.length; i++) {
			nums[i] = from + i;
		}

		return from(nums);
	}

	public static Set<Long> set(Numbers numbers) {
		long[] nums = numbers.toArray();
		Set<Long> set = new HashSet<Long>();

		for (Long num : nums) {
			set.add(num);
		}

		return set;
	}

	public static long[] arrFromTo(int from, int to) {
		long[] nums = new long[to - from + 1];

		for (int i = 0; i < nums.length; i++) {
			nums[i] = from + i;
		}

		return nums;
	}

	public static boolean validate(long[] nums, int start, int length) {
		long max = Long.MIN_VALUE;

		for (int i = start; i < start + length; i++) {
			long n = nums[i];
			Check.state(n > max, "Invalid numbers sequence, expected > " + max + ", found: " + n + " at " + (i - start)
					+ " of " + length);
			max = n;
		}

		return true;
	}

	public static Numbers unionAll(Numbers... numss) {
		if (numss.length == 0) {
			return none();
		}

		Numbers nums = numss[0];

		for (int i = 1; i < numss.length; i++) {
			nums = union(nums, numss[i]);
		}

		return nums;
	}

	public static Numbers intersectAll(Numbers[] numss) {
		if (numss.length == 0) {
			return none();
		}

		Numbers nums = numss[0];

		for (int i = 1; i < numss.length; i++) {
			nums = intersect(nums, numss[i]);
		}

		return nums;
	}

	public static Numbers union(Numbers nums1, Numbers nums2) {
		long[] arr1 = nums1.toArray();
		long[] arr2 = nums2.toArray();

		long[] arr = new long[arr1.length + arr2.length];
		int size = 0;

		long last = 0; // whatever

		int ind1 = 0;
		int ind2 = 0;

		while (true) {

			boolean done1 = ind1 >= arr1.length;
			boolean done2 = ind2 >= arr2.length;

			if (done1 && done2) {
				break;
			}

			if (done1) {
				int len = arr2.length - ind2;
				if (len > 0) {
					if (size > 0 && last == arr2[ind2]) {
						len--;
						ind2++;
					}
					System.arraycopy(arr2, ind2, arr, size, len);
					size += len;
				}
				break;
			}

			if (done2) {
				int len = arr1.length - ind1;
				if (len > 0) {
					if (size > 0 && last == arr1[ind1]) {
						len--;
						ind1++;
					}
					System.arraycopy(arr1, ind1, arr, size, len);
					size += len;
				}
				break;
			}

			long n1 = arr1[ind1];
			long n2 = arr2[ind2];

			if (n1 < n2) {
				if (size == 0 || last != n1) {
					arr[size++] = n1;
					last = n1;
				}
				ind1++;
			} else if (n1 > n2) {
				if (size == 0 || last != n2) {
					arr[size++] = n2;
					last = n2;
				}
				ind2++;
			} else {
				if (size == 0 || last != n1) {
					arr[size++] = n1;
					last = n1;
				}
				ind1++;
				ind2++;
			}
		}

		return from(arr, 0, size);
	}

	public static Numbers intersect(Numbers nums1, Numbers nums2) {
		long[] arr1 = nums1.toArray();
		long[] arr2 = nums2.toArray();

		// order: smaller, bigger
		if (arr1.length > arr2.length) {
			long[] tmp = arr1;
			arr1 = arr2;
			arr2 = tmp;
		}

		switch (arr1.length) {
		case 0:
			return none();

		case 1:
			long n1 = arr1[0];
			boolean contains = Arrays.binarySearch(arr2, n1) >= 0;
			return contains ? from(n1) : none();

		default:
			return intersect(arr1, arr2);
		}
	}

	private static Numbers intersect(long[] arr1, long[] arr2) {
		long[] arr = new long[Math.min(arr1.length, arr1.length)];
		int size = 0;

		int ind1 = 0;
		int ind2 = 0;

		while (true) {
			if (ind1 >= arr1.length || ind2 >= arr2.length) {
				break;
			}

			if (arr1[ind1] < arr2[ind2]) {
				ind1++;
			} else if (arr1[ind1] > arr2[ind2]) {
				ind2++;
			} else {
				arr[size++] = arr1[ind1];
				ind1++;
				ind2++;
			}
		}

		return from(arr, 0, size);
	}

	public static Numbers filter(Numbers numbers, Numbers filter) {
		Check.notNull(numbers, "numbers to be filtered");
		if (filter == null) {
			return numbers;
		} else {
			return intersect(numbers, filter);
		}
	}

	public static boolean equal(Numbers a, Numbers b) {
		if (a == null && b == null) {
			return true;
		}

		if (a == null || b == null) {
			return false;
		}

		return Arrays.equals(a.toArray(), b.toArray());
	}

	public static Numbers[] clone(Numbers[] nums) {
		Numbers[] copy = new Numbers[nums.length];

		for (int i = 0; i < copy.length; i++) {
			Numbers num = nums[i];
			copy[i] = num != null ? from(num.toArray()) : null;
		}

		return copy;
	}

	public static Numbers union(Collection<SortedSet<Long>> sets) {
		List<Numbers> allNums = new ArrayList<Numbers>();

		for (SortedSet<Long> numSet : sets) {
			allNums.add(from(numSet));
		}

		return unionAll(allNums.toArray(new Numbers[allNums.size()]));
	}

}
