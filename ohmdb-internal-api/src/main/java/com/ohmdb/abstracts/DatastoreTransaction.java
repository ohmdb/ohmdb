package com.ohmdb.abstracts;

import java.util.Map;
import java.util.Set;

import com.ohmdb.api.TransactionListener;

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

public interface DatastoreTransaction {

	void changed(long key);

	void write(long key, Object value);

	void delete(long key);

	void commit();

	void rollback();

	void addListener(TransactionListener listener);

	Map<Long, Object> changed();

	Set<Long> deleted();

	boolean isReadOnly();

	void done();

	void success();

}
