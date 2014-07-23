package com.ohmdb.transaction;

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

import org.testng.annotations.Test;

import com.ohmdb.test.SimpleParallel;
import com.ohmdb.test.TestCommons;
import com.ohmdb.test.ThreadPack;
import com.ohmdb.util.U;

public class TransactionBatchingTest extends TestCommons {

	private volatile boolean ready = false;

	@Test
	public void shouldBatchTransactions() {
		int threads = 1000;
		int iterations = 1;

		/*** INSERT IN PARALLEL TRANSACTIONS ***/

		ThreadPack pack = threads("read", threads, iterations, false, new SimpleParallel() {

			@Override
			public void run(int threadN, int cycleN) {
				blockingOp(threadN);
			}

		});
		pack.start();

		System.out.println("sleep");
		U.sleep(1000);
		ready = true;
		System.out.println("ready!");

		ThreadPack.finish(pack);
	}

	protected void blockingOp(int threadN) {
//		System.out.println("wait " + threadN);
		while (!ready) {

		}
//		System.out.println("done " + threadN);
	}

}
