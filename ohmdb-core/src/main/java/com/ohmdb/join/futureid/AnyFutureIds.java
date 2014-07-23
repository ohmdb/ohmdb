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

import com.ohmdb.abstracts.Any;
import com.ohmdb.numbers.Numbers;
import com.ohmdb.numbers.Nums;
import com.ohmdb.util.Check;

public class AnyFutureIds extends AbstractFutureIds {

	private final Any<?> any;

	public AnyFutureIds(Any<?> any) {
		Check.notNull(any, "any");
		this.any = any;
	}

	@Override
	public String toString() {
		return any.toString();
	}

	@Override
	public Numbers getIds() {
		return Nums.from(any.all());
	}

	@Override
	public boolean optional() {
		return true;
	}

	@Override
	public int size() {
		return any.size();
	}

}
