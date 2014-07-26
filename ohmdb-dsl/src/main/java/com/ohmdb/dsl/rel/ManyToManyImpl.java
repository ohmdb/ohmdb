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

import com.ohmdb.abstracts.RWRelation;
import com.ohmdb.api.ManyToMany;

public class ManyToManyImpl<FROM, TO> extends AbstractCommonRelation<FROM, TO> implements ManyToMany<FROM, TO> {

	public ManyToManyImpl(RWRelation relation) {
		super(relation);
	}

	@Override
	public ManyToMany<TO, FROM> inversed() {
		return new ManyToManyImpl<TO, FROM>(rel.inverse());
	}

	@Override
	public TO[] from(FROM from) {
		return froms_(from);
	}

	@Override
	public FROM[] to(TO to) {
		return tos_(to);
	}

	@Override
	public long[] from(long fromId) {
		return froms_(fromId);
	}

	@Override
	public long[] to(long toId) {
		return tos_(toId);
	}

}
