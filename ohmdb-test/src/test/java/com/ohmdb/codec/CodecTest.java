package com.ohmdb.codec;

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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.testng.annotations.Test;

import com.ohmdb.test.TestCommons;
import com.ohmdb.util.U;

public class CodecTest extends TestCommons {

	private ByteBuffer buf = ByteBuffer.allocateDirect(100000);

	@Test
	public void shoudEncodeAndDecodeNull() {
		verify(null, 1);
	}

	@Test
	public void shoudEncodeAndDecodeSimpleTypes() {
		verify(true, 2);
		verify(false, 2);

		verify((byte) 12, 2);
		verify((byte) -34, 2);

		verify((short) 12453, 3);
		verify((short) -24556, 3);

		verify(12345678, 5);
		verify(-12345678, 5);

		verify(1234567890123L, 9);
		verify(-1234567890123L, 9);

		verify(123.4567890123f, 5);
		verify(-1234567.890123f, 5);

		verify(123.4567890123, 9);
		verify(-1234567.890123, 9);
	}

	@Test
	public void shoudEncodeAndDecodeStrings() {
		verify("", 2);
		verify("a", 3);
		verify("xy", 4);

		for (int i = 0; i < 255; i++) {
			verify(rndStr(i), i + 2);
		}

		for (int i = 255; i < 999; i++) {
			verify(rndStr(i), i + 6);
		}
	}

	@Test
	public void shoudEncodeAndDecodeSpecialObjects() {
		verify(new Date(), 9);
		verify(new Date(213455354), 9);
		verify(new Date(123), 9);
	}

	@Test
	public void shoudEncodeAndDecodeArrays() {
		verify(new int[] {});
		verify(new int[] { 3, 5, 7 });
		verify(new byte[] { 0, 123 });
		verify(new String[] { "f", "bb" });
	}

	@Test
	public void shoudEncodeAndDecodeCollections() {
		verify(new HashMap<String, Integer>());

		HashMap<String, Integer> map = new HashMap<String, Integer>();
		map.put("aa", 123);
		map.put("bb", 456);
		verify(map);

		HashSet<String> set = new HashSet<String>();
		set.add("abc");
		set.add("xy");
		verify(set);

		List<String> list = new ArrayList<String>();
		list.add("abc");
		list.add("xy");
		verify(list);
	}

	private void verify(Object val) {
		verify(val, 0);
	}

	private void verify(Object val, int size) {
		buf.clear();
		assert buf.position() == 0;

		U.encode(val, buf);
		int bufN = buf.position();

		buf.flip();
		assert buf.position() == 0;
		assert buf.limit() == bufN;

		Object val2 = U.decode(buf);

		// the buffer should be fully read
		eq(buf.position(), bufN);

		eq(val, val2);

		if (size > 0) {
			eq(bufN, size);
		}
	}

}
