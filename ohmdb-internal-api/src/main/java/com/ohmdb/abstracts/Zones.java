package com.ohmdb.abstracts;

/*
 * #%L
 * ohmdb-internal-api
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

import java.util.Set;

public interface Zones {

	public abstract Set<Long> occupy(int num);

	public abstract void occupied(long position);

	public abstract void release(long position);

	public abstract void releaseAll(long... positions);

	public abstract void releaseAll(Set<Long> positions);

	public abstract void occupiedAll(Set<Long> positions);

}
