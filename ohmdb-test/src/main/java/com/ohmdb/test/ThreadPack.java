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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;

import com.ohmdb.api.OhmDB;
import com.ohmdb.util.U;

public class ThreadPack {

	public final Thread[] threads;

	private final int threadCount;

	// private final int cyclesCount;
	//
	// private final String name;

	private Throwable error;

	private final AtomicInteger finished = new AtomicInteger();

	private final List<Parallel> parallels = new LinkedList<Parallel>();

	public ThreadPack(String name, int threadCount, int cyclesCount) {
		// this.name = name;
		// this.cyclesCount = cyclesCount;
		this.threadCount = threadCount;
		threads = new Thread[threadCount];
	}

	public ThreadPack start() {
		// Measure.start(threadCount * cyclesCount);

		new Thread() {
			@Override
			public void run() {
				for (int i = 0; i < threads.length; i++) {
					threads[i].start();
				}
			}
		}.start();

		return this;
	}

	public boolean finished() {
		return error != null || finished.get() >= threadCount;
	}

	public void error(Throwable error) {
		error.printStackTrace();
		this.error = error;
		for (int i = 0; i < threads.length; i++) {
			threads[i].interrupt();
		}

	}

	public synchronized void done(Parallel parallel) {
		finished.incrementAndGet();
		parallels.add(parallel);
	}

	public static void finish(ThreadPack... packs) {
		while (true) {
			int n = 0;

			for (ThreadPack pack : packs) {
				if (pack.hasError()) {
					Assert.fail("Error in thread!", pack.getError());
				}

				if (pack.finished()) {
					n++;
				}
			}

			if (n == packs.length) {
				return;
			}

			U.sleep(10);
		}
	}

	public static void finishFirst(OhmDB db, ThreadPack... packs) {
		while (true) {
			for (int i = 0; i < packs.length; i++) {
				ThreadPack pack = packs[i];

				if (pack.hasError()) {
					Assert.fail("Error in thread!", pack.getError());
				}

				if (pack.finished()) {
					if (i == 0) {
						for (int j = 1; j < packs.length; j++) {
							packs[j].stop();
						}
						return;
					} else {
						Assert.fail("The first thread pack must finish before the others!");
					}
				}
			}

			U.sleep(1000);
		}
	}

	public void stop() {
		for (int i = 0; i < threads.length; i++) {
			threads[i].interrupt();
		}
	}

	private Throwable getError() {
		return error;
	}

	private boolean hasError() {
		return error != null;
	}

	public List<Parallel> getParallels() {
		return parallels;
	}

}
