package com.ohmdb.impl;

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

import com.ohmdb.api.OhmDB;
import com.ohmdb.api.Trigger;
import com.ohmdb.api.TriggerAction;
import com.ohmdb.api.TriggerCreator;
import com.ohmdb.api.TriggerOperation;

public class TriggerOperationImpl<E> implements TriggerOperation<E> {

	private final OhmDB db;
	private final Class<E> type;
	private final boolean before;
	private final TriggerAction[] actions;

	public TriggerOperationImpl(OhmDB db, Class<E> type, boolean before, TriggerAction... actions) {
		this.db = db;
		this.type = type;
		this.before = before;
		this.actions = actions;
	}

	@Override
	public TriggerCreator<E> or() {
		return new TriggerCreatorImpl<E>(db, type, before, actions);
	}

	@Override
	public void run(Trigger<E> trigger) {
		for (TriggerAction action : actions) {
			db.trigger(type, action, trigger);
		}
	}

}
