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

import com.ohmdb.test.TestCommons;

public class ClearAllRollbackTest extends TestCommons {

	@Test
	public void shouldRollbackClearAll() {
		initData10();

		for (int i = 0; i < 100; i++) {
			tx();

			persons.clear();
			books.clear();
			tags.clear();

			rollback();

			checkData10();
		}
	}

}
