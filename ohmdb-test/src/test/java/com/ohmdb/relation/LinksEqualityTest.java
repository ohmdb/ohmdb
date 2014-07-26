package com.ohmdb.relation;

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

import com.ohmdb.links.NoLinks;
import com.ohmdb.test.TestCommons;
import com.ohmdb.util.UTILS;

public class LinksEqualityTest extends TestCommons {

	@Test
	public void shouldMatchPaths() {
		eqlinks(new NoLinks(), UTILS.NO_PATHS);
		neq(links(ln(1)), UTILS.NO_PATHS);
		neq(UTILS.NO_PATHS, links(ln(77)));

		eqlinks(links(ln(1)), links(ln(1)));
		neq(links(ln(1)), links(ln(77)));

		eqlinks(links(ln(1, 2)), links(ln(1, 2)));
		neq(links(ln(1, 2)), links(ln(1)));
		neq(links(ln(1)), links(ln(1, 2)));

		eqlinks(links(ln(1, 2, 3), ln(7, 8)), links(ln(1, 2, 3), ln(7, 8)));
		neq(links(ln(1, 2, 3), ln(7, 99)), links(ln(1, 2, 3), ln(7, 8)));
		neq(links(ln(1, 2, 3), ln(7)), links(ln(1, 2, 3), ln(7, 8)));
		neq(links(ln(1, 2, 3)), links(ln(1, 2, 3), ln(7, 8)));
	}

	@Test
	public void shouldCheckEquality() {
		eqlinks(links(ln(1, 2, 3), ln(7, 8)), links(ln(1, 2, 3), ln(7, 8)));
	}

}
