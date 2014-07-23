package com.ohmdb.api;

/*
 * #%L
 * ohmdb-api
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

public enum JoinMode {

	INNER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER;

	public JoinMode inverse() {
		switch (this) {
		case INNER:
			return INNER;
		case LEFT_OUTER:
			return RIGHT_OUTER;
		case RIGHT_OUTER:
			return LEFT_OUTER;
		case FULL_OUTER:
			return FULL_OUTER;
		}

		throw new RuntimeException();
	}

}
