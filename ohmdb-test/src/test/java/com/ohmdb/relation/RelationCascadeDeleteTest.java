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

public class RelationCascadeDeleteTest extends PersonWroteBookFixture {

	@Test
	public void shouldDeleteRelationsOfDeletedRecord() {
		p1_x_b1_b2_$_p2_x_b2_b3();

		persons.delete(p1);

		p1().p2(b2, b3).p(1).b(2);
	}

	@Test
	public void shouldDeleteRelationsOfDeletedRecord2() {
		p1_x_b1_b2_$_p2_x_b2_b3();

		books.delete(b2);

		p1(b1).p2(b3).p(2).b(2);
	}

	@Test
	public void shouldRollbackDeletedRelationsOfDeletedRecord() {
		p1_x_b1_b2_$_p2_x_b2_b3();

		tx();

		persons.delete(p1);

		p1().p2(b2, b3).p(1).b(2);

		rollback();

		check_p1_x_b1_b2_$_p2_x_b2_b3();
	}

	@Test
	public void shouldRollbackDeletedRelationsOfDeletedRecord2() {
		p1_x_b1_b2_$_p2_x_b2_b3();

		tx();

		books.delete(b2);

		p1(b1).p2(b3).p(2).b(2);

		rollback();

		check_p1_x_b1_b2_$_p2_x_b2_b3();
	}

}
