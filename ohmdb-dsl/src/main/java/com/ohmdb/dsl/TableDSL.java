package com.ohmdb.dsl;

import com.ohmdb.abstracts.JokerCreator;
import com.ohmdb.api.Criteria;
import com.ohmdb.api.CustomIndex;
import com.ohmdb.api.Table;
import com.ohmdb.dsl.criteria.impl.ComplexCriteriaImpl;
import com.ohmdb.dsl.criteria.impl.CriteriaImpl;
import com.ohmdb.joker.JokerCreatorImpl;

public abstract class TableDSL<E> implements Table<E> {

	protected final JokerCreator jokerator = new JokerCreatorImpl();

	public <T> Criteria<E, T> where(T property) {
		return new CriteriaImpl<E, T>(this, jokerator.decode(property));
	}

	public <T> Criteria<E, T> where(String propertyName, Class<T> type) {
		return new CriteriaImpl<E, T>(this, propertyName);
	}

	public <T> Criteria<E, T> where(CustomIndex<E, T> indexer) {
		return new ComplexCriteriaImpl<E, T>(this, indexer);
	}

}
