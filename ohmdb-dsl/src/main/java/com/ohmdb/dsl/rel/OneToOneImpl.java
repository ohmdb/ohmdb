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
import com.ohmdb.api.OneToOne;

public class OneToOneImpl<FROM, TO> extends AbstractCommonRelation<FROM, TO> implements OneToOne<FROM, TO> {

	public OneToOneImpl(RWRelation relation) {
		super(relation);
	}

	@Override
	public OneToOne<TO, FROM> inversed() {
		return new OneToOneImpl<TO, FROM>(rel.inverse());
	}

	@Override
	public TO from(FROM from) {
		return from_(from);
	}

	@Override
	public FROM to(TO to) {
		return to_(to);
	}

	@Override
	public long from(long fromId) {
		return from_(fromId);
	}

	@Override
	public long to(long toId) {
		return to_(toId);
	}

}
