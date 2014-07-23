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
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;

import com.ohmdb.api.Links;
import com.ohmdb.links.LinksBuilder;
import com.ohmdb.links.LinksImpl;
import com.ohmdb.links.NoLinks;
import com.ohmdb.numbers.Nums;

public class LINKS {

	public static final Links NO_PATHS = new NoLinks();

	public static boolean equal(Links[] l1, Links[] l2) {
		if (l1.length != l2.length) {
			return false;
		}

		for (int i = 0; i < l1.length; i++) {
			if (!equal(l1[i], l2[i])) {
				return false;
			}
		}

		return true;
	}

	public static boolean equal(Links p1, Links p2) {
		if (p1 == p2) {
			return true;
		}

		if (p1 == null || p2 == null) {
			return false;
		}

		if (p1.size() != p2.size()) {
			return false;
		}

		int size = p1.size();
		for (int i = 0; i < size; i++) {
			if (p1.from(i) != p2.from(i) || !Arrays.equals(p1.to(i), p2.to(i))) {
				return false;
			}
		}

		return true;
	}

	public static String toString(Links paths) {
		if (paths == null) {
			return "null";
		}

		StringBuilder sb = new StringBuilder();
		sb.append("{");

		for (int i = 0; i < paths.size(); i++) {
			if (i > 0) {
				sb.append(", ");
			}

			sb.append(paths.from(i));
			long[] nums = paths.to(i);
			if (nums != null) {
				sb.append(" : ");
				sb.append(U.text(nums));
				sb.append(" ");
			}
		}

		sb.append("}");
		return sb.toString();
	}

	public static Links from(long[] keys, long[][] related) {
		assert keys.length == related.length;
		assert Nums.validate(keys, 0, keys.length);

		return new LinksImpl(keys, related);
	}

	public static Links fromTos(long[][] fromTos) {
		int size = fromTos.length;
		long[] keys = new long[size];
		long[][] numbers = new long[size][];

		for (int i = 0; i < size; i++) {
			long[] fromTo = fromTos[i];
			keys[i] = fromTo[0];

			int len = fromTo.length - 1;
			numbers[i] = new long[len];
			System.arraycopy(fromTo, 1, numbers[i], 0, len);
		}

		return from(keys, numbers);
	}

	public static Links from(SortedMap<Long, SortedSet<Long>> links) {
		int size = links.size();
		long[] keys = new long[size];
		long[][] numbers = new long[size][];

		int i = 0;
		for (Entry<Long, SortedSet<Long>> fromTo : links.entrySet()) {
			keys[i] = fromTo.getKey();
			SortedSet<Long> values = fromTo.getValue();
			numbers[i] = Nums.arrFrom(values);
			i++;
		}

		return from(keys, numbers);
	}

	public static LinksBuilder builder() {
		return new LinksBuilder();
	}

}
