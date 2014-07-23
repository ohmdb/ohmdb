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

import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.ohmdb.api.Links;
import com.ohmdb.util.LINKS;

public class LinksBuilder {

	private SortedMap<Long, SortedSet<Long>> links = new TreeMap<Long, SortedSet<Long>>();

	private SortedSet<Long> linkedTos = new TreeSet<Long>();

	public void link(long from, long to) {
		SortedSet<Long> ll = links.get(from);

		if (ll == null) {
			ll = new TreeSet<Long>();
			links.put(from, ll);
		}

		if (to != -1) {
			ll.add(to);

			if (from != -1) {
				linkedTos.add(to);
			}
		}
	}

	public Links build() {
		SortedSet<Long> rights = links.get(-1L);

		if (rights != null) {
			rights.removeAll(linkedTos);
		}

		return LINKS.from(links);
	}

}
