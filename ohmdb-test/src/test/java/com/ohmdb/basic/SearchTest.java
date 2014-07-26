package com.ohmdb.basic;

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

import org.testng.annotations.Test;

import com.ohmdb.api.Op;
import com.ohmdb.api.SearchCriteria;
import com.ohmdb.dsl.rel.SearchCriteriaImpl;
import com.ohmdb.test.TestCommons;

public class SearchTest extends TestCommons {

	@Test
	public void shouldMatchByCriteriaConjunction() {
		initSchema();
		initIndexing();

		persons.insert(person("a", 2));
		persons.insert(person("b", 1));
		persons.insert(person("two", 100));
		persons.insert(person("a", 2));
		persons.insert(person("b", 200));
		persons.insert(person("two", 300));

		SearchCriteria crit1 = db.crit("name", Op.EQ, "a");
		SearchCriteria crit2 = db.crit("age", Op.EQ, 2);
		SearchCriteria criteria = SearchCriteriaImpl.conjunction(crit1, crit2);

		long[] ids = persons.find(criteria);

		eqnums(ids, 0, 3);
	}

	@Test
	public void shouldMatchByCriteriaDisjunction() {
		initSchema();
		initIndexing();

		persons.insert(person("a", 2));
		persons.insert(person("b", 1));
		persons.insert(person("two", 100));
		persons.insert(person("a", 2));
		persons.insert(person("b", 200));
		persons.insert(person("two", 300));

		SearchCriteria crit1 = db.crit("name", Op.EQ, "a");
		SearchCriteria crit2 = db.crit("age", Op.EQ, 300);
		SearchCriteria criteria = SearchCriteriaImpl.disjunction(crit1, crit2);

		long[] ids = persons.find(criteria);

		eqnums(ids, 0, 3, 5);
	}

	@Test
	public void shouldBeFineOnEmptyTable() {
		initSchema();
		initIndexing();

		SearchCriteria crit1 = db.crit("name", Op.EQ, "a");
		SearchCriteria crit2 = db.crit("age", Op.EQ, 12345);
		SearchCriteria criteria = SearchCriteriaImpl.conjunction(crit1, crit2);

		long[] ids = persons.find(criteria);

		eq(ids.length, 0);
	}

}
