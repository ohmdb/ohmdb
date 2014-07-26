package com.ohmdb.bean;

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
import com.ohmdb.util.U;

public class BeanIdTest extends TestCommons {

	static abstract class A1 {
		long id;
	}

	static class A2 extends A1 {
	}

	static abstract class B1 {
		long getId() {
			return 345;
		}
	}

	static class B2 extends B1 {
	}

	static abstract class C1 {
	}

	static class C2 extends C1 {
		long id() {
			return 987;
		}
	}

	@Test
	public void chouldGetIdFromField() throws Exception {
		A2 a = new A2();
		a.id = 1234;
		eq(U.getId(a), 1234);
	}

	@Test
	public void chouldGetIdFromGetter() throws Exception {
		B2 b = new B2();
		eq(U.getId(b), 345);
	}

	@Test
	public void chouldGetIdFromGetter2() throws Exception {
		C2 c = new C2();
		eq(U.getId(c), 987);
	}

}
