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

import com.ohmdb.TableInternals;
import com.ohmdb.api.Ids;
import com.ohmdb.api.Table;
import com.ohmdb.dsl.impl.AbstractUpdaters;

public class RecordsImpl<E> extends AbstractUpdaters<E> implements Ids<E> {

	private final long[] ids;

	@SuppressWarnings("unchecked")
	public RecordsImpl(Table<E> table, long[] ids) {
		super(((TableInternals<E>) table).jokerator());
		this.ids = ids;
	}

	@Override
	public long[] ids() {
		return ids;
	}

	@Override
	public int size() {
		return ids.length;
	}

}
