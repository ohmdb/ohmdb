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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SynchronizationCheck extends TestCommons {

	private final Set<Object> writing = new HashSet<Object>();

	private final Map<Object, Long> reading = new HashMap<Object, Long>();

	public synchronized void startWrite(Object key) {
		isFalse(writing.contains(key));
		isFalse(reading.containsKey(key));

		writing.add(key);
	}

	public synchronized void endWrite(Object key) {
		isTrue(writing.contains(key));

		writing.remove(key);
	}

	public synchronized void startRead(Object key) {
		isFalse(writing.contains(key));

		Long counter = reading.get(key);

		if (counter == null) {
			counter = 1L;
		} else {
			counter++;
		}

		reading.put(key, counter);
	}

	public synchronized void endRead(Object key) {
		Long counter = reading.get(key);

		isTrue(counter != null);

		if (counter > 1) {
			reading.put(key, counter - 1);
		} else {
			reading.remove(key);
		}
	}

}
