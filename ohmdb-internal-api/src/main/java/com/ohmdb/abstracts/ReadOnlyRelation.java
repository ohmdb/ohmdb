package com.ohmdb.abstracts;

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

import java.util.List;

import com.ohmdb.api.Table;

public interface ReadOnlyRelation {

	Numbers linksFrom(long id);

	Numbers linksTo(long id);

	String info();

	String name();

	Table<?> from();

	Table<?> to();

	List<long[]> exportFromTo();

	List<long[]> exportToFrom();

	ReadOnlyRelation inverse();

	boolean hasLink(long from, long to);

	int fromSize();

	int toSize();

	Numbers froms();

	Numbers tos();

}
