package com.ohmdb.filestore;

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

import com.ohmdb.impl.OhmDBImpl;
import com.ohmdb.util.Errors;

public class NoDataStore extends AbstractDataStore implements DataStore {

	private final OhmDBImpl ohmDBImpl;

	public NoDataStore(OhmDBImpl ohmDBImpl) {
		this.ohmDBImpl = ohmDBImpl;
	}

	@Override
	public void write(long key, Object value) {
		checkActive();
	}

	@Override
	public long getFileSize() {
		throw Errors.notExpected();
	}

	@Override
	public FilestoreTransaction transaction() {
		checkActive();
		return new FilestoreTransaction(this, ohmDBImpl);
	}

	@Override
	public void delete(long key) {
		checkActive();
	}

	@Override
	public void clear() {
		checkActive();
	}

	@Override
	public void commit(FilestoreTransaction tx) {
		tx.success();
	}

	@Override
	public void rollback(FilestoreTransaction tx) {
	}

}
