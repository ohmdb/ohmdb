package com.ohmdb.dsl.rel;

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

import com.ohmdb.api.CustomIndex;
import com.ohmdb.api.Op;
import com.ohmdb.api.SearchCriterion;
import com.ohmdb.util.Check;

public class SearchCriterionImpl implements SearchCriterion {

	private final Op op;
	private final Object value;
	private final String columnName;
	private final CustomIndex<?, ?> indexer;

	public SearchCriterionImpl(String columnName, Op op, Object value) {
		Check.notNull(columnName, "column name");
		Check.notNull(op, "operator");
		Check.notNull(value, "value");

		this.op = op;
		this.value = value;
		this.columnName = columnName;
		this.indexer = null;
	}

	public SearchCriterionImpl(CustomIndex<?, ?> indexer, Op op, Object value) {
		Check.notNull(indexer, "indexer");
		Check.notNull(op, "operator");
		Check.notNull(value, "value");

		this.op = op;
		this.value = value;
		this.columnName = null;
		this.indexer = indexer;
	}

	@Override
	public Op op() {
		return op;
	}

	@Override
	public Object value() {
		return value;
	}

	@Override
	public String columnName() {
		return columnName;
	}

	@Override
	public CustomIndex<?, ?> indexer() {
		return indexer;
	}

	@Override
	public String toString() {
		if (columnName != null) {
			return columnName + " " + op.sign() + " " + value;
		} else {
			return indexer + " " + op.sign() + " " + value;
		}
	}

}
