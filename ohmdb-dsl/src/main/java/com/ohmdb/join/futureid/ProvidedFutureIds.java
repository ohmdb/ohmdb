package com.ohmdb.join.futureid;

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

import com.ohmdb.abstracts.Numbers;
import com.ohmdb.api.Ids;
import com.ohmdb.numbers.Nums;
import com.ohmdb.util.Check;

public class ProvidedFutureIds extends AbstractFutureIds {

	private final Ids<?> ids;

	public ProvidedFutureIds(Ids<?> ids) {
		Check.notNull(ids, "ids");
		this.ids = ids;
	}

	@Override
	public String toString() {
		return ids.toString();
	}

	@Override
	public Numbers getIds() {
		return Nums.from(ids.ids());
	}

	@Override
	public boolean optional() {
		return false;
	}

	@Override
	public int size() {
		return ids.size();
	}

}
