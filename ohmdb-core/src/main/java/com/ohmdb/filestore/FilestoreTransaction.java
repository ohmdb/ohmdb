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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ohmdb.api.TransactionListener;
import com.ohmdb.util.Check;
import com.ohmdb.util.Errors;

public class FilestoreTransaction implements DatastoreTransaction {

	final DataStore store;

	final DataSource source;

	final Set<Long> changed = new HashSet<Long>(1);

	final Map<Long, Object> values = new HashMap<Long, Object>(1);

	final Set<Long> deleted = new HashSet<Long>(1);

	final List<TransactionListener> listeners = new ArrayList<TransactionListener>(1);

	private TxState state = TxState.INITIAL;

	FilestoreTransaction(DataStore store, DataSource source) {
		Check.notNull(store, "store");
		Check.notNull(source, "source");
		this.store = store;
		this.source = source;
	}

	@Override
	public synchronized void changed(long key) {
		if (state != TxState.INITIAL && state != TxState.RESET) {
			throw Errors.rte("Cannot write to freezed transaction!");
		}

		changed.add(key);
	}

	@Override
	public synchronized void write(long key, Object value) {
		if (state != TxState.INITIAL && state != TxState.RESET) {
			throw Errors.rte("Cannot write to freezed transaction!");
		}

		values.put(key, value);
	}

	@Override
	public synchronized void delete(long key) {
		if (state != TxState.INITIAL && state != TxState.RESET) {
			throw Errors.rte("Cannot write to freezed transaction!");
		}

		values.remove(key);
		changed.remove(key);
		deleted.add(key);
	}

	@Override
	public synchronized void commit() {
		state = TxState.COMMITING;

		// the read transactions need to be commited only if they have listeners
		if (!isReadOnly() || !listeners.isEmpty()) {

			// update values for the registered changed keys
			for (long key : changed) {
				values.put(key, source.read(key));
			}

			changed.clear();
			store.commit(this);
		}
	}

	@Override
	public synchronized void rollback() {
		state = TxState.ROLLING_BACK;

		store.rollback(this);
	}

	public synchronized void addListener(TransactionListener listener) {
		Check.arg(!listeners.contains(listener), "Listener already registered for this transaction!");
		listeners.add(listener);
	}

	synchronized void success() {
		for (TransactionListener listener : listeners) {
			listener.onSuccess();
		}
	}

	public synchronized boolean isReadOnly() {
		return changed.isEmpty() && values.isEmpty() && deleted.isEmpty();
	}

	public synchronized void done() {
		state = TxState.DONE;
	}

}
