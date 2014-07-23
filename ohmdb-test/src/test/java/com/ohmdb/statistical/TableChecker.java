package com.ohmdb.statistical;

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

import com.ohmdb.test.TableShadow;

public abstract class TableChecker<E> {

	public final Class<E> clazz;
	public final String name;

	public TableChecker(String name, Class<E> clazz) {
		this.name = name;
		this.clazz = clazz;
	}

	public abstract void check(TableShadow<E> table, int iteration);

	public String getName() {
		return name;
	}

	public Class<E> getClazz() {
		return clazz;
	}

	@Override
	public String toString() {
		return "TableChecker [clazz=" + clazz + ", name=" + name + "]";
	}

}
