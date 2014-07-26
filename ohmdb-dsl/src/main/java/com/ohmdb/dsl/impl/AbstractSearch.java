package com.ohmdb.dsl.impl;

/*
 * #%L
 * ohmdb-dsl
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

import com.ohmdb.api.BoundableSearch;
import com.ohmdb.api.ParameterBinding;
import com.ohmdb.api.SearchCriteria;
import com.ohmdb.api.Table;

public abstract class AbstractSearch<E> extends AbstractSearchActions<E> {

	public AbstractSearch(Table<E> table) {
		super(table);
	}

	@Override
	public <T> BoundableSearch<E> bind(ParameterBinding<T> binding) {
		return new BindSearchImpl<E>(table, this, binding);
	}

	protected abstract SearchCriteria query(BindSearchImpl<E> search);

}
