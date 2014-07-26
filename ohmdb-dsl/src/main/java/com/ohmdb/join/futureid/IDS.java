package com.ohmdb.join.futureid;

/*
 * #%L
 * ohmdb-dsl
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
import com.ohmdb.abstracts.FutureIds;
import com.ohmdb.abstracts.Numbers;
import com.ohmdb.api.Ids;
import com.ohmdb.api.Search;
import com.ohmdb.api.Table;
import com.ohmdb.util.Check;

public class IDS {

	public static FutureIds futureIds(Numbers ids) {
		return new PreloadedFutureIds(ids);
	}

	public static FutureIds[] futureIds(Numbers[] ids) {

		FutureIds[] futureIds = new FutureIds[ids.length];

		for (int i = 0; i < ids.length; i++) {
			Check.notNull(ids[i], "IDs #" + i);
			futureIds[i] = new PreloadedFutureIds(ids[i]);
		}

		return futureIds;
	}

	public static FutureIds[] futureIds(Ids<?>[] providers) {
		FutureIds[] futureIds = new FutureIds[providers.length];

		for (int i = 0; i < providers.length; i++) {
			Ids<?> provider = providers[i];
			if (provider instanceof Search<?>) {
				futureIds[i] = new SearchFutureIds((Search<?>) provider);
			} else if (provider instanceof Table<?>) {
				futureIds[i] = new TableFutureIds((Table<?>) provider);
			} else if (provider instanceof Any<?>) {
				futureIds[i] = new AnyFutureIds((Any<?>) provider);
			} else {
				futureIds[i] = new ProvidedFutureIds(provider);
			}
		}

		return futureIds;
	}

	public static <T> FutureIds futureIds(Any<T> any) {
		return new AnyFutureIds(any);
	}
	
}
