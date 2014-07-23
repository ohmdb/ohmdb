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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.ohmdb.abstracts.RWRelation;
import com.ohmdb.abstracts.RelationInternals;
import com.ohmdb.numbers.Nums;

public class RelationCheck extends TestCommons {

	private final Multimap<Long, Long> fromTo = HashMultimap.<Long, Long> create();

	private final Multimap<Long, Long> toFrom = HashMultimap.<Long, Long> create();

	public synchronized void connect(long fromId, long toId) {
		if (!fromTo.containsEntry(fromId, toId)) {
			fromTo.put(fromId, toId);
			toFrom.put(toId, fromId);
		}
	}

	public synchronized void disconnect(long fromId, long toId) {
		fromTo.remove(fromId, toId);
		toFrom.remove(toId, fromId);
	}

	public synchronized void deleteFrom(long fromId) {
		Collection<Long> removed = fromTo.removeAll(fromId);
		for (Long toId : removed) {
			toFrom.remove(toId, fromId);
		}
	}

	public synchronized void deleteTo(long toId) {
		Collection<Long> removed = toFrom.removeAll(toId);
		for (Long fromId : removed) {
			fromTo.remove(fromId, toId);
		}
	}

	public void validate(RWRelation relation) {
		RelationInternals internals = (RelationInternals) relation;

		// System.out.println("+ EXPECTING " + fromTo.keySet());
		// System.out.println("+ HAVING " + internals.froms());
		eq(internals.fromSize(), fromTo.keySet().size());

		// System.out.println("+ EXPECTING " + toFrom.keySet());
		// System.out.println("+ HAVING " + internals.tos());
		eq(internals.toSize(), toFrom.keySet().size());

		for (Long key : fromTo.keys()) {
			Set<Long> real = Nums.set(relation.linksFrom(key));
			Set<Long> expected = new HashSet<Long>(fromTo.get(key));

			eq(real, expected);
		}

		for (Long key : toFrom.keys()) {
			Set<Long> real = Nums.set(relation.linksTo(key));
			Set<Long> expected = new HashSet<Long>(toFrom.get(key));

			eq(real, expected);
		}
	}

	public void print() {
		System.out.println(fromTo);
		System.out.println(toFrom);
	}

	@Override
	public String toString() {
		return fromTo + " ::: " + toFrom;
	}

}
