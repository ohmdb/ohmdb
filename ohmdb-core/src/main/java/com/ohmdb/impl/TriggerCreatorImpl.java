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

import java.util.Arrays;

import com.ohmdb.api.OhmDB;
import com.ohmdb.api.TriggerAction;
import com.ohmdb.api.TriggerCreator;
import com.ohmdb.api.TriggerOperation;
import com.ohmdb.util.Errors;
import com.ohmdb.util.U;

public class TriggerCreatorImpl<E> implements TriggerCreator<E> {

	private final OhmDB db;
	private final Class<E> type;
	private final boolean before;
	private final TriggerAction[] actions;

	public TriggerCreatorImpl(OhmDB db, Class<E> type, boolean before, TriggerAction... actions) {
		this.db = db;
		this.type = type;
		this.before = before;
		this.actions = actions;
	}

	@Override
	public TriggerOperation<E> inserted() {
		return op(before ? TriggerAction.BEFORE_INSERT : TriggerAction.AFTER_INSERT);
	}

	@Override
	public TriggerOperation<E> updated() {
		return op(before ? TriggerAction.BEFORE_UPDATE : TriggerAction.AFTER_UPDATE);
	}

	@Override
	public TriggerOperation<E> deleted() {
		return op(before ? TriggerAction.BEFORE_DELETE : TriggerAction.AFTER_DELETE);
	}

	@Override
	public TriggerOperation<E> read() {
		return op(before ? TriggerAction.BEFORE_READ : TriggerAction.AFTER_READ);
	}

	private TriggerOperation<E> op(TriggerAction action) {
		if (U.indexOf(actions, action, 0) >= 0) {
			throw Errors.illegalArgument("Duplicate trigger action: " + action);
		}

		TriggerAction[] mergedActions = Arrays.copyOf(actions, actions.length + 1);
		mergedActions[mergedActions.length - 1] = action;
		return new TriggerOperationImpl<E>(db, type, before, mergedActions);
	}

}
