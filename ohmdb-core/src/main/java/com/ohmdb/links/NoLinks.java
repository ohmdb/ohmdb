package com.ohmdb.links;

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

import com.ohmdb.api.Links;
import com.ohmdb.util.Errors;

public class NoLinks implements Links {

	@Override
	public int size() {
		return 0;
	}

	@Override
	public long from(int index) {
		throw Errors.notExpected();
	}

	@Override
	public long[] to(int index) {
		throw Errors.notExpected();
	}

	@Override
	public Links inverse() {
		return this;
	}

	@Override
	public String toString() {
		return "NO LINKS";
	}
	
}
