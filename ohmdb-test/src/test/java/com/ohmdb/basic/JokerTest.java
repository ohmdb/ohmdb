package com.ohmdb.basic;

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

import com.ohmdb.api.Table;
import com.ohmdb.joker.PropertyCodeException;
import com.ohmdb.test.TestCommons;

public class JokerTest extends TestCommons {

	public static class Foo {

		public boolean a1;
		public boolean a2;

		public Boolean b1;
		public Boolean b2;
		public Boolean b3;

		public byte c;
		public char d;
		public short e;
		public int f;
		public long g;
		public float h;
		public double i;

		public Byte j;
		public Character k;
		public Short l;
		public Integer m;
		public Long n;
		public Float o;
		public Double p;

		public String q1;
		public String q2;

		public Object r1;
		public Object r2;

		public Nested s1;
		public Nested s2;

		@Override
		public String toString() {
			return "Foo [a1=" + a1 + ", a2=" + a2 + ", b1=" + b1 + ", b2=" + b2 + ", b3=" + b3 + ", c=" + c + ", d="
					+ d + ", e=" + e + ", f=" + f + ", g=" + g + ", h=" + h + ", i=" + i + ", j=" + j + ", k=" + k
					+ ", l=" + l + ", m=" + m + ", n=" + n + ", o=" + o + ", p=" + p + ", q1=" + q1 + ", q2=" + q2
					+ ", r1=" + r1 + ", r2=" + r2 + ", s1=" + s1 + ", s2=" + s2 + "]";
		}

	}

	private static class Nested {
		public String a;
		public int b;

		private Nested() {
		}

		@Override
		public String toString() {
			return "Nested [a=" + a + ", b=" + b + "]";
		}
	}

	public static class Bar {
		public boolean x;
		public boolean y;
		public boolean z;
	}

	@Test
	public void shouldEncodeAndDecodeProperties() {
		Table<Foo> foo = db.table(Foo.class);

		Foo joker = foo.queryHelper();

		eq(foo.nameOf(joker.a1), "a1");
		eq(foo.nameOf(joker.a2), "a2");

		eq(foo.nameOf(joker.b1), "b1");
		eq(foo.nameOf(joker.b2), "b2");
		eq(foo.nameOf(joker.b3), "b3");

		eq(foo.nameOf(joker.c), "c");
		eq(foo.nameOf(joker.d), "d");
		eq(foo.nameOf(joker.e), "e");
		eq(foo.nameOf(joker.f), "f");
		eq(foo.nameOf(joker.g), "g");
		eq(foo.nameOf(joker.h), "h");
		eq(foo.nameOf(joker.i), "i");
		eq(foo.nameOf(joker.j), "j");
		eq(foo.nameOf(joker.k), "k");
		eq(foo.nameOf(joker.l), "l");
		eq(foo.nameOf(joker.m), "m");
		eq(foo.nameOf(joker.n), "n");
		eq(foo.nameOf(joker.o), "o");
		eq(foo.nameOf(joker.p), "p");

		eq(foo.nameOf(joker.q1), "q1");
		eq(foo.nameOf(joker.q2), "q2");

		eq(foo.nameOf(joker.r1), "r1");
		eq(foo.nameOf(joker.r2), "r2");

		eq(foo.nameOf(joker.s1), "s1");
		eq(foo.nameOf(joker.s2), "s2");

		System.out.println(joker);
	}

	@Test(expectedExceptions = { PropertyCodeException.class })
	public void shouldFailOnManyBooleans() {
		Table<Bar> bar = table(Bar.class);
		bar.queryHelper(); // should fail
	}

	@Test(expectedExceptions = { PropertyCodeException.class })
	public void shouldFailOnNullCode() {
		Table<Foo> foo = table(Foo.class);
		foo.queryHelper();
		foo.nameOf(null);
	}

	@Test(expectedExceptions = { PropertyCodeException.class })
	public void shouldFailOnNegativeCode() {
		Table<Foo> foo = table(Foo.class);
		foo.queryHelper();
		foo.nameOf(-123);
	}

	@Test(expectedExceptions = { PropertyCodeException.class })
	public void shouldFailOnTooBigCode() {
		Table<Foo> foo = table(Foo.class);
		foo.queryHelper();
		foo.nameOf(256);
	}

	@Test(expectedExceptions = { PropertyCodeException.class })
	public void shouldFailOnUnknownObjectCode() {
		Table<Foo> foo = table(Foo.class);
		foo.queryHelper();
		foo.nameOf(new Object());
	}

	@Test(expectedExceptions = { PropertyCodeException.class })
	public void shouldFailOnBadStringCode() {
		Table<Foo> foo = table(Foo.class);
		foo.queryHelper();
		foo.nameOf("asd");
	}

	@Test
	public void shouldWorkOnNonInitializedProperties() {
		Table<Foo> foo = table(Foo.class);
		foo.nameOf(false);
	}

	@Test
	public void shouldReuseHelper() {
		Table<Foo> foo = table(Foo.class);
		isTrue(foo.queryHelper() == foo.queryHelper());
	}

}
