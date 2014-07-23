package com.ohmdb.test;

/*
 * #%L
 * ohmdb-test
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

import java.util.concurrent.atomic.AtomicInteger;

import com.ohmdb.api.TransactionListener;

public class IncTransactionListener implements TransactionListener {

	private final AtomicInteger n;

	public IncTransactionListener(AtomicInteger n) {
		this.n = n;
	}

	@Override
	public void onSuccess() {
		n.incrementAndGet();
	}

	@Override
	public void onError(Exception e) {
		System.err.println("TRANSACTION ERROR");
		e.printStackTrace(System.err);
	}

}
