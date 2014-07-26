package com.ohmdb.util;

/*
 * #%L
 * ohmdb-utils
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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class Errors {

	public static RuntimeException rte(String message, Throwable cause) {
		return new RuntimeException(message, cause);
	}

	public static RuntimeException rte(Throwable cause) {
		return new RuntimeException(cause);
	}

	public static RuntimeException rte(String message, Object... args) {
		return new RuntimeException(String.format(message, args));
	}

	public static RuntimeException rte(String message, Throwable cause, Object... args) {
		return new RuntimeException(String.format(message, args), cause);
	}

	public static void check(boolean condition, String message, Object... args) {
		if (!condition) {
			throw new IllegalStateException(String.format(message, args));
		}
	}

	public static IllegalStateException illegalState(String message, Object... args) {
		throw new IllegalStateException(String.format(message, args));
	}

	public static IllegalStateException illegalArgument(String message, Object... args) {
		throw new IllegalArgumentException(String.format(message, args));
	}

	public static void notNull(Object value, String desc) {
		if (value == null) {
			throw new IllegalStateException(String.format("The %s must NOT be null!", desc));
		}
	}

	public static RuntimeException notReady() {
		return new RuntimeException("Not yet implemented!");
	}

	public static RuntimeException notSupported() {
		return new RuntimeException("The operation is not supported by this implementation!");
	}

	public static IllegalStateException notExpected() {
		return new IllegalStateException("The operation is not supposed to be called!");
	}

	public static String stackTrace(Throwable e) {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		e.printStackTrace(new PrintStream(output));
		return output.toString();
	}

}
