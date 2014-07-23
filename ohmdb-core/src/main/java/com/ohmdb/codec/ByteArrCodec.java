package com.ohmdb.codec;

/*
 * #%L
 * ohmdb-core
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

public class ByteArrCodec implements StoreCodec<byte[]> {

	@Override
	public void encode(ByteBuffer buf, byte[] bytes) {
		// buf.put((byte) 65); // A
		// buf.put((byte) 66); // B
		// buf.put((byte) 67); // C
		// buf.put((byte) 68); // D

		buf.putInt(bytes.length);
		buf.put(bytes);

		// buf.put((byte) 'D');
		// buf.put((byte) 'O');
		// buf.put((byte) 'N');
		// buf.put((byte) 'E');
	}

	@Override
	public byte[] decode(ByteBuffer buf) {
		// buf.getInt(); // ABCD

		int len = buf.getInt();
		byte[] bytes = new byte[len];
		buf.get(bytes);

		// buf.getInt(); // DONE

		return bytes;
	}

}
