package com.ohmdb.abstracts;

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

import com.ohmdb.filestore.StoreLoader;
import com.ohmdb.util.Errors;

public class DataLoader implements StoreLoader {

	private final LoadedData data = new LoadedData();

	@Override
	public void set(long key, Object value) {
		if (key < 0) {
			throw Errors.notExpected();
			// data.setSchema(value);
		} else {
			// System.out.println("SET " + key + " = " + value);
			data.setData(key, value);
		}
	}

	@Override
	public void delete(long key) {
		// System.out.println("DELETE " + key);
		data.delete(key);
	}

	public LoadedData getData() {
		return data;
	}

}
