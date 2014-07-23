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

public interface Join {

	<FROM, TO> Join join(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to);

	<FROM, TO> Join leftJoin(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to);

	<FROM, TO> Join rightJoin(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to);

	<FROM, TO> Join fullJoin(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to);

	boolean exists();

	JoinResult all();

	Join bind(ParameterBinding<?> binding);

}
