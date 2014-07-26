package com.ohmdb.util;

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

public class Check {

	public static void state(boolean expectedCondition) {
		if (!expectedCondition) {
			throw new IllegalStateException();
		}
	}

	public static void state(boolean expectedCondition, String message, Object... args) {
		if (!expectedCondition) {
			throw new IllegalStateException(String.format(message, args));
		}
	}

	public static void notNulls(Object[] objs) {
		for (Object value : objs) {
			if (value == null) {
				throw new IllegalStateException(String.format("The argument must NOT be null!"));
			}
		}
	}

	public static void notNulls2(Object... objs) {
		for (Object value : objs) {
			if (value == null) {
				throw new IllegalStateException(String.format("The argument must NOT be null!"));
			}
		}
	}

	public static void notNull(Object value, String desc) {
		if (value == null) {
			throw new IllegalStateException(String.format("The %s must NOT be null!", desc));
		}
	}

	public static void arg(boolean expectedCondition, String message, Object... args) {
		if (!expectedCondition) {
			throw new IllegalArgumentException(String.format(message, args));
		}
	}

}
