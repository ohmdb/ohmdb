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

import com.ohmdb.fixture.PersonWroteBookFixture;
import com.ohmdb.util.UTILS;

public class RelationsTest extends PersonWroteBookFixture {

	@Test
	public void shouldKeepRelations() {
		init();

		UTILS.link(wrote, p1, nums(b1, b2));
		UTILS.link(wrote, p2, nums(b2, b3));

		p1(b1, b2).p2(b2, b3).p(2);
		b1(p1).b2(p1, p2).b3(p2).b(3);
	}

	@Test
	public void shouldRelinkRelations() {
		init();

		UTILS.link(wrote, p1, nums(b1, b2));
		UTILS.link(wrote, p1, nums(b1, b2));
		UTILS.link(wrote, p1, nums(b1, b2));
	}

	@Test
	public void shouldKeepInverseRelations() {
		init();

		UTILS.link(writtenBy, b1, nums(p1));
		UTILS.link(writtenBy, b2, nums(p1, p2));
		UTILS.link(writtenBy, b3, nums(p2));

		p1(b1, b2).p2(b2, b3).p(2);
		b1(p1).b2(p1, p2).b3(p2).b(3);
	}

	@Test
	public void shouldLinkAndDelink() {
		init();

		UTILS.link(wrote, p1, nums(b1));
		UTILS.link(wrote, p2, nums(b3));
		UTILS.link(wrote, nums(p1, p2), b2);

		p1(b1, b2).p2(b2, b3).p(2);
		b1(p1).b2(p1, p2).b3(p2).b(3);

		UTILS.delink(wrote, p1, nums(b1));
		UTILS.delink(wrote, p2, nums(b3));
		UTILS.delink(wrote, nums(p1, p2), b2);

		p1().p2().p(0);
		b1().b2().b(0);
	}

	@Test
	public void shouldLinkAndDelinkInversed() {
		init();

		UTILS.link(writtenBy, b1, nums(p1));
		UTILS.link(writtenBy, b3, nums(p2));
		UTILS.link(writtenBy, b2, nums(p1, p2));

		p1(b1, b2).p2(b2, b3).p(2);
		b1(p1).b2(p1, p2).b3(p2).b(3);

		setVerbose(true);

		System.out.println(wrote.info());
		UTILS.delink(writtenBy, b1, nums(p1));
		System.out.println(wrote.info());
		UTILS.delink(writtenBy, b3, nums(p2));
		System.out.println(":::" + wrote.info());

		System.out.println("===A");
		UTILS.delink(wrote, p1, nums(b2));
		System.out.println("===B");
		UTILS.delink(wrote, p2, nums(b2));
		System.out.println("===CCCC");
		// UTILS.delink(writtenBy, b2, nums(p1, p2));

		p1().p2().p(0);
		b1().b2().b(0);
	}

	@Test
	public void shouldNotRevertNonLinked() {
		init();

		UTILS.link(writtenBy, b1, nums(p1));

		p1(b1).p(1);

		tx();

		UTILS.link(writtenBy, b1, nums(p1));

		rollback();

		p1(b1).p(1);
	}

}
