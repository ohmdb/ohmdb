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

public class SynchronizationCheck extends TestCommons {

	Thread writter;

	int writeCount = 0;

	int readCount = 0;

	public synchronized void startWrite(Object key) {
		isTrue(writter == Thread.currentThread() || notWriting());
		
		writter = Thread.currentThread();
		writeCount++;
	}

	public synchronized void endWrite(Object key) {
		isTrue(writter == Thread.currentThread());
		isTrue(writeCount > 0);
		
		writeCount--;

		if (writeCount == 0) {
			writter = null;
		}
	}

	public synchronized void startRead(Object key) {
		isTrue(writter == Thread.currentThread() || notWriting());
		readCount++;
	}

	public synchronized void endRead(Object key) {
		isTrue(writter == Thread.currentThread() || notWriting());
		isTrue(readCount > 0);
		readCount--;
	}

	private boolean notWriting() {
		return writter == null && writeCount == 0;
	}

}
