package com.ohmdb;

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

import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.ohmdb.abstracts.DbInsider;
import com.ohmdb.abstracts.JokerCreator;
import com.ohmdb.api.Trigger;
import com.ohmdb.api.TriggerAction;
import com.ohmdb.api.Visitor;
import com.ohmdb.bean.PropertyInfo;

public interface TableInternals<E> {

	void fill(long id, String columnName, Object value);

	ReentrantReadWriteLock getLock();

	void commit();

	void rollback();

	Class<E> getClazz();

	void setInsider(DbInsider insider);

	JokerCreator jokerator();

	void updateObj(Object entity);

	void addTrigger(TriggerAction action, Trigger<E> trigger);

	void forEach(Visitor<E> visitor);

	void forEach(long[] ids, Visitor<E> visitor);

	PropertyInfo[] props();

}
