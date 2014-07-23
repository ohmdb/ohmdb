package com.ohmdb.filestore;

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

import java.util.Comparator;

public class AreaSpaceComparator implements Comparator<Area> {

	@Override
	public int compare(Area a, Area b) {
		long diff = a.getLength() - b.getLength();
		return sign(diff != 0 || a.order < 0 || b.order < 0 ? diff : a.order - b.order);
	}

	private int sign(long n) {
		return n > 0 ? 1 : n < 0 ? -1 : 0;
	}
}
