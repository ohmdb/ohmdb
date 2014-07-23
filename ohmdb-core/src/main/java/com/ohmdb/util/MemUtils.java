package com.ohmdb.util;

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

public class MemUtils {

	public static Object flexyInsert(Object array, long num) {
		if (array instanceof int[]) {
			int[] numbers = (int[]) array;
			if (num <= Integer.MAX_VALUE && num >= Integer.MIN_VALUE) {
				return insert(numbers, (int) num);
			} else {
				long[] numbers2 = intToLongArray(numbers);
				return insert(numbers2, num);
			}
		} else if (array instanceof long[]) {
			long[] numbers = (long[]) array;
			return insert(numbers, num);
		} else {
			throw Errors.notSupported();
		}
	}

	/**
	 * Return new array without the specified element, or NULL instead of empty
	 * array!
	 */
	public static Object flexyRemove(Object array, long num) {
		if (array == null) {
			return null;
		} else if (array instanceof int[]) {
			if (num <= Integer.MAX_VALUE && num >= Integer.MIN_VALUE) {
				int[] arr = (int[]) array;
				int[] shorter = MemUtils.removeFirstInSorted(arr, (int) num);
				return shorter.length > 0 ? shorter : null;
			} else {
				return array;
			}
		} else if (array instanceof long[]) {
			long[] arr = (long[]) array;
			long[] shorter = MemUtils.removeFirstInSorted(arr, num);
			return shorter.length > 0 ? shorter : null;
		} else {
			throw Errors.illegalArgument("Unsupported array type!");
		}
	}

	public static boolean flexyContains(Object array, long num) {
		// TODO: improve all instanceof X[] checks - global REACHED_LONG FLAG!
		if (array instanceof int[]) {
			int[] numbers = (int[]) array;
			if (num <= Integer.MAX_VALUE && num >= Integer.MIN_VALUE) {
				return Arrays.binarySearch(numbers, (int) num) >= 0;
			} else {
				long[] numbers2 = intToLongArray(numbers);
				return Arrays.binarySearch(numbers2, num) >= 0;
			}
		} else if (array instanceof long[]) {
			long[] numbers = (long[]) array;
			return Arrays.binarySearch(numbers, num) >= 0;
		} else {
			throw Errors.notSupported();
		}
	}

	private static long[] intToLongArray(int[] numbers) {
		long[] array = new long[numbers.length];

		for (int i = 0; i < numbers.length; i++) {
			array[i] = numbers[i];
		}

		return array;
	}

	public static int[] insert(int[] arr, int num) {
		int index = Arrays.binarySearch(arr, num);

		if (index < 0) {
			int pos = -index - 1;
			return insertAt(arr, num, pos);
		} else {
			// throw Errors.rte("It's already there!");
			return arr;
		}
	}

	public static int[] insertAt(int[] arr, int num, int pos) {
		int[] bigger = new int[arr.length + 1];

		// copy left
		System.arraycopy(arr, 0, bigger, 0, pos);

		// insert
		bigger[pos] = num;

		// copy right
		System.arraycopy(arr, pos, bigger, pos + 1, arr.length - pos);

		return bigger;
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

	public static int[][] insertAt(int[][] arrays, int[] subarray, int pos) {
		int[][] bigger = new int[arrays.length + 1][];

		// copy left
		System.arraycopy(arrays, 0, bigger, 0, pos);

		// insert
		bigger[pos] = subarray;

		// copy right
		System.arraycopy(arrays, pos, bigger, pos + 1, arrays.length - pos);

		return bigger;
	}

	public static int[][] insertAtAndSplit(int[] arr, int num, int pos) {
		int totalSize = arr.length + 1;
		int sizeL = totalSize / 2;
		int sizeR = totalSize - sizeL;

		int[] left = new int[sizeL];
		int[] right = new int[sizeR];

		if (pos < sizeL) {
			// copy left
			System.arraycopy(arr, 0, left, 0, pos);

			// insert
			left[pos] = num;

			// copy right
			System.arraycopy(arr, pos, left, pos + 1, sizeL - pos - 1);

			System.arraycopy(arr, sizeL - 1, right, 0, sizeR);

		} else {

			// copy left
			System.arraycopy(arr, 0, left, 0, sizeL);

			// copy right
			System.arraycopy(arr, sizeL, right, 0, pos - sizeL);

			// insert
			right[pos - sizeL] = num;

			// copy right
			System.arraycopy(arr, pos, right, pos - sizeL + 1, totalSize - pos - 1);

		}

		return new int[][] { left, right };
	}

	public static int[] removeFirstInSorted(int[] arr, int value) {
		int index = Arrays.binarySearch(arr, value);

		if (index < 0) {
			return arr;
		}

		int[] smaller = new int[arr.length - 1];

		// copy left
		System.arraycopy(arr, 0, smaller, 0, index);

		// copy right
		System.arraycopy(arr, index + 1, smaller, index, arr.length - index - 1);

		return smaller;
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

	// private static void debug(String msg, int[] arr) {
	// System.out.println(" * " + msg + Arrays.toString(arr));
	// }
	//
	// private static void debug(String msg, long[] arr) {
	// System.out.println(" * " + msg + Arrays.toString(arr));
	// }

}
