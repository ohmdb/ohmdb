package com.ohmdb.links;

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

import com.ohmdb.api.Links;
import com.ohmdb.util.LINKS;

public class LinksImpl implements Links {

	private long[] keys;
	private long[][] related;

	public LinksImpl(long[] keys, long[][] related) {
		this.keys = keys;
		this.related = related;
	}

	@Override
	public int size() {
		return keys.length;
	}

	@Override
	public long from(int index) {
		return keys[index];
	}

	@Override
	public long[] to(int index) {
		return related[index];
	}

	@Override
	public Links inverse() {
		LinksBuilder builder = LINKS.builder();

		for (int i = 0; i < size(); i++) {
			long from = from(i);
			long[] toNums = to(i);

			if (from == -1) {
				for (long to : toNums) {
					builder.link(to, -1);
				}
			} else {
				if (toNums.length == 0) {
					builder.link(-1, from);
				} else {
					for (long to : toNums) {
						builder.link(to, from);
					}
				}
			}
		}

		return builder.build();
	}

	@Override
	public final String toString() {
		return LINKS.toString(this);
	}

}
