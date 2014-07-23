package com.ohmdb.join;

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

import com.ohmdb.api.Links;
import com.ohmdb.test.TestCommons;

public class LinksTest extends TestCommons {

	@Test
	public void shouldInvertLinks1() {
		Links ln = links(ln(-1, 100, 200), ln(0, 10, 20, 30), noln(1), ln(2, 20), noln(13));

		Links inv = ln.inverse();
		eqlinks(inv, links(ln(-1, 1, 13), ln(10, 0), ln(20, 0, 2), ln(30, 0), noln(100), noln(200)));

		eqlinks(inv.inverse(), ln);
		eqlinks(inv.inverse().inverse(), inv);
		eqlinks(inv.inverse().inverse().inverse(), ln);
	}

}
