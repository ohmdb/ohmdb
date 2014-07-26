package com.ohmdb.transaction;

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

import java.util.Arrays;

import com.ohmdb.abstracts.DatastoreTransaction;
import com.ohmdb.api.Transaction;
import com.ohmdb.api.TransactionException;
import com.ohmdb.api.TransactionListener;
import com.ohmdb.util.Errors;
import com.ohmdb.util.U;

public class TransactionImpl implements Transaction, TransactionInternals, TransactionListener {

	private final Transactor transactor;

	private final DatastoreTransaction storeTx;

	private TransactionStatus status = TransactionStatus.INITIAL;

	private volatile boolean finished = false;

	private volatile TransactionException failure;

	public TransactionImpl(Transactor transactor, DatastoreTransaction storeTx) {
		this.transactor = transactor;
		this.storeTx = storeTx;
	}

	public synchronized void begin() {
		expected(TransactionStatus.INITIAL);

		status = TransactionStatus.WAITING;
		transactor.begin(this);
		status = TransactionStatus.STARTED;
	}

	@Override
	public synchronized void commit() {
		expected(TransactionStatus.STARTED);

		status = TransactionStatus.COMMITING;
		transactor.commit(this);
		status = TransactionStatus.COMMITED;
	}

	@Override
	public synchronized void rollback() {
		expected(TransactionStatus.STARTED);

		status = TransactionStatus.ROLLING_BACK;
		transactor.rollback(this);
		status = TransactionStatus.ROLLED_BACK;
	}

	@Override
	public DatastoreTransaction getStoreTx() {
		return storeTx;
	}

	private void expected(TransactionStatus... expectedStatuses) {
		for (TransactionStatus expected : expectedStatuses) {
			if (expected == status) {
				return;
			}
		}

		Errors.illegalState("Current transaction status is %s, but expected statuses are: %s", status,
				Arrays.toString(expectedStatuses));
	}

	@Override
	public void addListener(TransactionListener listener) {
		storeTx.addListener(listener);
	}

	@Override
	public void sync() throws TransactionException {
		addListener(this);

		while (!finished) {
			U.sleep(5);
		}

		if (failure != null) {
			throw failure;
		}
	}

	@Override
	public void onSuccess() {
		finished = true;
	}

	@Override
	public void onError(Exception e) {
		finished = true;
		failure = new TransactionException(e);
	}

}
