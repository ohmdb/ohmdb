package com.ohmdb.demo;

/*
 * #%L
 * ohmdb-demo
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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

import com.ohmdb.util.Measure;

public class FileBenchmark {

	private static final int SIZE = 5 * 1024 * 1024;

	private static final int FILE_SIZE = 1500 * 1024 * 1024;

	static final Random rnd = new Random();

	public static void main(String[] args) throws IOException {

		ByteBuffer BUF = ByteBuffer.allocateDirect(64);

		String s = args.length > 2 ? args[2] : "ohmdb-demo.db";

		RandomAccessFile ff = new RandomAccessFile(s, "rw");

		int count = args.length > 0 ? Integer.parseInt(args[0]) : 100;
		int mm = args.length > 1 ? Integer.parseInt(args[1]) : 10;

		for (int kk = 0; kk < 10; kk++) {
			Measure.start(count);

			for (int i = 0; i < count; i++) {

				int pos = rnd.nextInt(FILE_SIZE / SIZE) * SIZE;

				if (mm > 7000) {
					mm = SIZE / 64;
					for (int j = 0; j < mm; j++) {
						ff.seek(pos + j * 64);
						BUF.rewind();
						write(ff.getChannel(), BUF, BUF.capacity());
					}
				} else {
					for (int j = 0; j < mm; j++) {
						int off = rnd.nextInt(SIZE / 64);
						ff.seek(pos + off);
						BUF.rewind();
						write(ff.getChannel(), BUF, BUF.capacity());
					}
				}
			}

			ff.getChannel().force(false);

			Measure.finish("random writes in " + mm + " * 64B");
		}

		ff.close();
	}

	private static void write(FileChannel fc, ByteBuffer out, int count) throws IOException {
		int real = 0;
		while (out.hasRemaining()) {
			real += fc.write(out);
		}
		assert count == real;
	}

}
