package com.ohmdb.abstracts;

/*
 * #%L
 * ohmdb-internal-api
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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class LoadedData {

	private final Map<Long, Object> values = new HashMap<Long, Object>();

	@SuppressWarnings("unused")
	private Object schema;

	public synchronized void setData(long key, Object value) {
		values.put(key, value);
	}

	public synchronized void delete(long key) {
		values.remove(key);
	}

	public synchronized void setSchema(Object value) {
		this.schema = value;
	}

	public synchronized void fillData(DataImporter db) {
		nice("FILLING " + values.size() + " entries");
		// for (Entry<Long, Object> ent : values.entrySet()) {
		// System.out.println(ent.getKey() + " :: " + ent.getValue());
		// }
		// nice(values.toString());
		// nice("-------------------");

		for (Entry<Long, Object> entry : values.entrySet()) {
			long id = entry.getKey();
			Object value = entry.getValue();
			db.importRecord(id, (byte[]) value);
		}

		values.clear();

		nice("END FILL");
	}

	private void nice(String msg) {
		// System.out.println(msg);
	}

	@Override
	public String toString() {
		return values.size() + " records";
	}

	public int count() {
		return values.size();
	}

}
