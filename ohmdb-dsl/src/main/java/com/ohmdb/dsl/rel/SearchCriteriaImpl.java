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

import java.util.List;

import com.ohmdb.api.CustomIndex;
import com.ohmdb.api.Op;
import com.ohmdb.api.SearchCriteria;
import com.ohmdb.api.SearchCriteriaKind;
import com.ohmdb.api.SearchCriterion;
import com.ohmdb.util.Errors;
import com.ohmdb.util.U;

public class SearchCriteriaImpl implements SearchCriteria {

	private final SearchCriteria[] criteria;
	private SearchCriterion criterion;
	private SearchCriteriaKind kind;

	public SearchCriteriaImpl(SearchCriteria[] criteria, SearchCriteriaKind kind) {
		this.criteria = criteria;
		this.criterion = null;
		this.kind = kind;
	}

	public SearchCriteriaImpl(SearchCriterion criterion) {
		this.criteria = null;
		this.criterion = criterion;
		this.kind = SearchCriteriaKind.CRITERION;
	}

	@Override
	public SearchCriteria[] criteria() {
		return criteria;
	}

	@Override
	public String toString() {
		switch (kind) {
		case CONJUNCTION:
			return "(" + U.join(criteria, " AND ") + ")";
		case DISJUNCTION:
			return "(" + U.join(criteria, " AND ") + ")";
		case CRITERION:
			return criterion.toString();

		default:
			throw Errors.notExpected();
		}
	}

	public static SearchCriteria conjunction(SearchCriteria... criteria) {
		if (criteria.length == 1) {
			return criteria[0];
		} else {
			return new SearchCriteriaImpl(criteria, SearchCriteriaKind.CONJUNCTION);
		}
	}

	public static SearchCriteria disjunction(SearchCriteria... criteria) {
		if (criteria.length == 1) {
			return criteria[0];
		} else {
			return new SearchCriteriaImpl(criteria, SearchCriteriaKind.DISJUNCTION);
		}
	}

	public static SearchCriteria conjunction(List<SearchCriteria> criteria) {
		return conjunction(criteria.toArray(new SearchCriteria[criteria.size()]));
	}

	public static SearchCriteria disjunction(List<SearchCriteria> criteria) {
		return disjunction(criteria.toArray(new SearchCriteria[criteria.size()]));
	}

	public static SearchCriteria single(String columnName, Op op, Object value) {
		return new SearchCriteriaImpl(new SearchCriterionImpl(columnName, op, value));
	}

	public static SearchCriteria single(CustomIndex<?, ?> indexer, Op op, Object value) {
		return new SearchCriteriaImpl(new SearchCriterionImpl(indexer, op, value));
	}

	@Override
	public SearchCriterion criterion() {
		return criterion;
	}

	@Override
	public SearchCriteriaKind kind() {
		return kind;
	}

}
