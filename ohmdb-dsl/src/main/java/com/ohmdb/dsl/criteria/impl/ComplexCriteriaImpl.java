package com.ohmdb.dsl.criteria.impl;

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

import com.ohmdb.api.Criteria;
import com.ohmdb.api.CustomIndex;
import com.ohmdb.api.Op;
import com.ohmdb.api.Parameter;
import com.ohmdb.api.Search;
import com.ohmdb.api.Table;
import com.ohmdb.dsl.impl.CriteriaChain;
import com.ohmdb.dsl.impl.SearchImpl;

public class ComplexCriteriaImpl<E, T> implements Criteria<E, T> {

	protected final Table<E> table;

	private final CustomIndex<E, T> indexer;

	private final CriteriaChain<E> chain;

	public ComplexCriteriaImpl(Table<E> table, CustomIndex<E, T> indexer) {
		this.table = table;
		this.indexer = indexer;
		this.chain = null;
	}

	public ComplexCriteriaImpl(CriteriaChain<E> chain, Table<E> table, CustomIndex<E, T> indexer) {
		this.chain = chain;
		this.table = table;
		this.indexer = indexer;
	}

	@Override
	public Search<E> eq(T value) {
		return new SearchImpl<E>(table, Op.EQ, indexer, value, chain);
	}

	@Override
	public Search<E> neq(T value) {
		return new SearchImpl<E>(table, Op.NEQ, indexer, value, chain);
	}

	@Override
	public Search<E> lt(T value) {
		return new SearchImpl<E>(table, Op.LT, indexer, value, chain);
	}

	@Override
	public Search<E> lte(T value) {
		return new SearchImpl<E>(table, Op.LTE, indexer, value, chain);
	}

	@Override
	public Search<E> gt(T value) {
		return new SearchImpl<E>(table, Op.GT, indexer, value, chain);
	}

	@Override
	public Search<E> gte(T value) {
		return new SearchImpl<E>(table, Op.GTE, indexer, value, chain);
	}

	@Override
	public Search<E> eq(Parameter<T> param) {
		return new SearchImpl<E>(table, Op.EQ, indexer, param, chain);
	}

	@Override
	public Search<E> neq(Parameter<T> param) {
		return new SearchImpl<E>(table, Op.NEQ, indexer, param, chain);
	}

	@Override
	public Search<E> lt(Parameter<T> param) {
		return new SearchImpl<E>(table, Op.LT, indexer, param, chain);
	}

	@Override
	public Search<E> lte(Parameter<T> param) {
		return new SearchImpl<E>(table, Op.LTE, indexer, param, chain);
	}

	@Override
	public Search<E> gt(Parameter<T> param) {
		return new SearchImpl<E>(table, Op.GT, indexer, param, chain);
	}

	@Override
	public Search<E> gte(Parameter<T> param) {
		return new SearchImpl<E>(table, Op.GTE, indexer, param, chain);
	}

}
