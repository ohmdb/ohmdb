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

import com.ohmdb.abstracts.LockManager;
import com.ohmdb.api.Transaction;
import com.ohmdb.impl.OhmDBImpl;
import com.ohmdb.impl.OhmDBStats;
import com.ohmdb.util.Check;

public class TransactorImpl implements Transactor {

	private final OhmDBImpl db;

	private final LockManager lockManager;

	private final OhmDBStats stats;

	private Transaction transaction;

	public TransactorImpl(OhmDBImpl db, LockManager lockManager, OhmDBStats stats) {
		this.db = db;
		this.lockManager = lockManager;
		this.stats = stats;
	}

	@Override
	public void begin(Transaction transaction) {
		lockManager.globalWriteLock();

		stats.transactions++;
		this.transaction = transaction;
	}

	@Override
	public void rollback(Transaction transaction) {
//		System.out.println("!!!!! ROLLING BACK !!!!!");
		Check.arg(this.transaction == transaction, "Invalid transaction!");

		db.rollback();

		TransactionInternals internals = (TransactionInternals) transaction;
		internals.getStoreTx().rollback();

		this.transaction = null;
		stats.rollbacks++;

		lockManager.globalWriteUnlock();
	}

	@Override
	public void commit(Transaction transaction) {
		Check.arg(this.transaction == transaction, "Invalid transaction!");

		db.commit();

		TransactionInternals internals = (TransactionInternals) transaction;
		internals.getStoreTx().commit();

		this.transaction = null;
		stats.commits++;

		lockManager.globalWriteUnlock();
	}

	@Override
	public Transaction getTransaction() {
		return transaction;
	}

}
