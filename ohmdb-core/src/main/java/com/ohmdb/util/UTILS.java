package com.ohmdb.util;

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
import com.ohmdb.abstracts.RWRelation;
import com.ohmdb.abstracts.RelationInternals;
import com.ohmdb.api.Ids;
import com.ohmdb.api.Search;
import com.ohmdb.api.Table;
import com.ohmdb.exception.OhmDBException;
import com.ohmdb.join.futureid.AnyFutureIds;
import com.ohmdb.join.futureid.FutureIds;
import com.ohmdb.join.futureid.PreloadedFutureIds;
import com.ohmdb.join.futureid.ProvidedFutureIds;
import com.ohmdb.join.futureid.SearchFutureIds;
import com.ohmdb.join.futureid.TableFutureIds;
import com.ohmdb.numbers.Numbers;

public class UTILS {

	public static final boolean PROD = false;

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

	public static OhmDBException err(String msg) {
		return new OhmDBException(msg);
	}

	public static <T> FutureIds futureIds(Any<T> any) {
		return new AnyFutureIds(any);
	}

	public static void link(RWRelation rel, long from, Numbers tos) {
		for (long to : tos.toArray()) {
			rel.link(from, to);
		}
	}

	public static void delink(RWRelation rel, long from, Numbers tos) {
		for (long to : tos.toArray()) {
			rel.delink(from, to);
		}
	}

	public static void link(RelationInternals rel, Numbers froms, long to) {
		for (long from : froms.toArray()) {
			rel.link(from, to);
		}
	}

	public static void delink(RelationInternals rel, Numbers froms, long to) {
		for (long from : froms.toArray()) {
			rel.delink(from, to);
		}
	}

	public static long getId(Object obj) {
		Object id = ClassUtils.getPropValue(obj, "id");

		if (id == null) {
			throw Errors.rte("The field 'id' cannot be null!");
		}

		if (id instanceof Long) {
			Long num = (Long) id;
			return num;
		} else {
			throw Errors.rte("The field 'id' must have type 'long', but it has: " + id.getClass());
		}
	}

	public static long[] getIds(Object... objs) {
		long[] ids = new long[objs.length];

		for (int i = 0; i < objs.length; i++) {
			ids[i] = getId(objs[i]);
		}

		return ids;
	}

}
