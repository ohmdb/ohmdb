package com.ohmdb.impl;

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

import java.util.HashMap;
import java.util.Map;

import com.ohmdb.abstracts.IdAddress;
import com.ohmdb.abstracts.IdColl;

public class MapBackedIdColl implements IdColl {

	private final Map<Long, IdAddress> addresses = new HashMap<Long, IdAddress>();

	private long counter;

	@SuppressWarnings("unused")
	private final OhmDBStats stats;

	public MapBackedIdColl(OhmDBStats stats) {
		this.stats = stats;
	}

	@Override
	public synchronized boolean has(long id) {
		return addresses.containsKey(id);
	}

	@Override
	public synchronized IdAddress get(long id) {
		return addresses.get(id);
	}

	@Override
	public synchronized void set(long id, IdAddress addr) {
		addresses.put(id, addr);
	}

	@Override
	public synchronized void delete(long id) {
		addresses.remove(id);
	}

	@Override
	public synchronized void clear() {
		addresses.clear();
	}

	@Override
	public synchronized boolean isValid(long id) {
		return id >= 0 && id < counter && has(id);
	}

	@Override
	public synchronized long newId() {
		return counter++;
	}

	@Override
	public synchronized void registerId(long id) {
		counter = Math.max(counter, id + 1);
	}

	@Override
	public synchronized void cancelId(long id) {
		if (id == counter - 1) {
			counter--;
		}
	}

}
