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

import com.ohmdb.abstracts.Index;
import com.ohmdb.api.Mapper;
import com.ohmdb.index.ComplexIndex;

public class ComplexIndexImpl<E> implements ComplexIndex<E> {

	private final Mapper<E, Object> mapper;

	private final Index index;

	public ComplexIndexImpl(Mapper<E, Object> mapper, Index index) {
		this.mapper = mapper;
		this.index = index;
	}

	@Override
	public void add(E entity, long id) {
		Object value = mapper.map(entity);

		if (value instanceof Object[]) {
			Object[] arr = (Object[]) value;
			for (Object item : arr) {
				index.add(item, id);
			}
		} else {
			index.add(value, id);
		}
	}

	@Override
	public void remove(E oldEntity, long id) {
		Object oldValue = mapper.map(oldEntity);

		if (oldValue instanceof Object[]) {
			Object[] arr = (Object[]) oldValue;
			for (Object item : arr) {
				index.remove(item, id);
			}
		} else {
			index.remove(oldValue, id);
		}
	}

}
