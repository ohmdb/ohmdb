package com.ohmdb.dsl.impl;

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

import java.util.ArrayList;
import java.util.List;

import com.ohmdb.api.Criteria;
import com.ohmdb.api.CustomIndex;
import com.ohmdb.api.Op;
import com.ohmdb.api.Parameter;
import com.ohmdb.api.ParameterBinding;
import com.ohmdb.api.Search;
import com.ohmdb.api.SearchCriteria;
import com.ohmdb.api.Table;
import com.ohmdb.dsl.criteria.impl.ComplexCriteriaImpl;
import com.ohmdb.dsl.criteria.impl.CriteriaImpl;
import com.ohmdb.dsl.impl.CriteriaChain.AND_OR;
import com.ohmdb.dsl.rel.SearchCriteriaImpl;
import com.ohmdb.util.Check;
import com.ohmdb.util.Errors;

public class SearchImpl<E> extends AbstractSearch<E> implements Search<E> {

	private final Op op;
	private final String propertyName;
	private final Object value;
	private final Parameter<?> param;
	private final CriteriaChain<E> prev;
	private final SearchCriteria query;
	private final CustomIndex<E, ?> indexer;

	public SearchImpl(Table<E> table, Op op, String propertyName, Object value, CriteriaChain<E> prev) {
		super(table);
		this.op = op;
		this.indexer = null;
		this.propertyName = propertyName;
		this.value = value;
		this.prev = prev;
		this.param = null;
		this.query = query(null);
	}

	public SearchImpl(Table<E> table, Op op, String propertyName, Parameter<?> param, CriteriaChain<E> prev) {
		super(table);
		this.op = op;
		this.indexer = null;
		this.propertyName = propertyName;
		this.prev = prev;
		this.value = null;
		this.param = param;
		this.query = query(null);
	}

	public SearchImpl(Table<E> table, Op op, CustomIndex<E, ?> indexer, Object value, CriteriaChain<E> prev) {
		super(table);
		this.op = op;
		this.indexer = indexer;
		this.propertyName = null;
		this.prev = prev;
		this.value = value;
		this.param = null;
		this.query = query(null);
	}

	public SearchImpl(Table<E> table, Op op, CustomIndex<E, ?> indexer, Parameter<?> param, CriteriaChain<E> prev) {
		super(table);
		this.op = op;
		this.indexer = indexer;
		this.propertyName = null;
		this.prev = prev;
		this.value = null;
		this.param = param;
		this.query = query(null);
	}

	private CriteriaChain<E> AND() {
		return new CriteriaChain<E>(AND_OR.AND, this);
	}

	private CriteriaChain<E> OR() {
		return new CriteriaChain<E>(AND_OR.OR, this);
	}

	@Override
	public <T> Criteria<E, T> and(T property) {
		return new CriteriaImpl<E, T>(AND(), table, table.nameOf(property));
	}

	@Override
	public <T> Criteria<E, T> or(T property) {
		return new CriteriaImpl<E, T>(OR(), table, table.nameOf(property));
	}

	@Override
	public <T> Criteria<E, T> and(String propertyName, Class<T> type) {
		return new CriteriaImpl<E, T>(AND(), table, propertyName);
	}

	@Override
	public <T> Criteria<E, T> or(String propertyName, Class<T> type) {
		return new CriteriaImpl<E, T>(OR(), table, propertyName);
	}

	@Override
	public <T> Criteria<E, T> and(CustomIndex<E, T> indexer) {
		return new ComplexCriteriaImpl<E, T>(AND(), table, indexer);
	}

	@Override
	public <T> Criteria<E, T> or(CustomIndex<E, T> indexer) {
		return new ComplexCriteriaImpl<E, T>(OR(), table, indexer);
	}

	@Override
	protected long[] findIDs() {
		return table.find(query);
	}

	@Override
	protected E[] findEntities() {
		return table.getAll(findIDs());
	}

	@Override
	protected SearchCriteria query(BindSearchImpl<E> bindingSearch) {
		List<SearchCriteria> disjunction = new ArrayList<SearchCriteria>();
		List<SearchCriteria> conjunction = new ArrayList<SearchCriteria>();

		SearchImpl<E> search = this;
		while (true) {
			Object value = search.param != null ? val(search.param, bindingSearch) : search.value;

			if (search.propertyName != null) {
				conjunction.add(SearchCriteriaImpl.single(search.propertyName, search.op, value));
			} else if (search.indexer != null) {
				conjunction.add(SearchCriteriaImpl.single(search.indexer, search.op, value));
			} else {
				throw Errors.notExpected();
			}

			if (search.prev != null) {
				switch (prev.and_or) {
				case AND:
					break;

				case OR:
					disjunction.add(SearchCriteriaImpl.conjunction(conjunction));
					conjunction = new ArrayList<SearchCriteria>();
					break;

				default:
					throw Errors.notExpected();
				}
				search = search.prev.search;
			} else {
				break;
			}
		}

		disjunction.add(SearchCriteriaImpl.conjunction(conjunction));
		conjunction = new ArrayList<SearchCriteria>();

		return SearchCriteriaImpl.disjunction(disjunction);
	}

	private Object val(Parameter<?> param, BindSearchImpl<E> bindingSearch) {
		BindSearchImpl<E> search = bindingSearch;

		while (search != null) {
			ParameterBinding<?> bind = search.binding;

			Check.notNull(bind, "parameter binding");

			if (bind.param().equals(param)) {
				return bind.value();
			}

			if (search.prev instanceof BindSearchImpl<?>) {
				search = (BindSearchImpl<E>) search.prev;
			} else {
				break;
			}
		}

		return param;
	}

	@Override
	public String toString() {
		return table.name() + "[" + query.toString() + "]";
	}

	@Override
	public long[] ids() {
		return table.find(query);
	}

}
