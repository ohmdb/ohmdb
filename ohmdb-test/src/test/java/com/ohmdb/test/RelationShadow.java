package com.ohmdb.test;

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

import com.ohmdb.abstracts.DbInsider;
import com.ohmdb.abstracts.RWRelation;
import com.ohmdb.abstracts.RelationInternals;
import com.ohmdb.util.ProxyUtil;

public class RelationShadow extends TestCommons {

	private final String relationName;

	private final TestInsider insider;

	private RWRelation relation;

	private RelationInternals internals;

	public RelationShadow(String relationName, TestInsider insider) {
		this.relationName = relationName;
		this.insider = insider;
	}

	public void setRelation(RWRelation relation) {
		this.relation = relation;
		this.internals = (RelationInternals) relation;
		this.internals.setInsider(ProxyUtil.tracer(DbInsider.class, insider));
	}

	public String getRelationName() {
		return relationName;
	}

	public void link(long fromId, long toId) {
		relation.link(fromId, toId);
	}

	public void delink(long fromId, long toId) {
		relation.delink(fromId, toId);
	}

	public void deleteFrom(long fromId) {
		relation.deleteFrom(fromId);
	}

	public void deleteTo(long toId) {
		relation.deleteTo(toId);
	}

	public void validate() {
		System.out.println(" - validating relation: " + relationName);
		System.out.println("from size: " + internals.fromSize() + ", to size: " + internals.toSize());

		insider.rel(relationName).validate(relation);
	}

}
