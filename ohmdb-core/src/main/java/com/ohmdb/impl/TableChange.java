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

import com.ohmdb.bean.PropertyInfo;

public class TableChange {

	static enum ChangeType {
		INSERT_ROW, INSERT_CELL, DELETE, SET_CELL, DELETE_CELL
	}

	ChangeType type;
	long id;
	PropertyInfo prop;
	Object oldValue;
	Object value;
	int row;
	boolean reuseRow;

	public TableChange(ChangeType type, long id, PropertyInfo prop, Object oldValue, Object value, int row,
			boolean reuseRow) {
		this.type = type;
		this.id = id;
		this.prop = prop;
		this.oldValue = oldValue;
		this.value = value;
		this.row = row;
		this.reuseRow = reuseRow;
	}

	public static TableChange insertRow(long id, int row, boolean reuseRow) {
		return new TableChange(ChangeType.INSERT_ROW, id, null, null, null, row, reuseRow);
	}

	public static TableChange insertCell(PropertyInfo prop, long id, int row, Object value) {
		return new TableChange(ChangeType.INSERT_CELL, id, prop, null, value, row, false);
	}

	public static TableChange delete(long id, int row) {
		return new TableChange(ChangeType.DELETE, id, null, null, null, row, false);
	}

	public static TableChange set(long id, PropertyInfo prop, Object oldValue, Object value) {
		return new TableChange(ChangeType.SET_CELL, id, prop, oldValue, value, 0, false);
	}

	public static TableChange deleteCell(long id, PropertyInfo prop, int row, Object value) {
		return new TableChange(ChangeType.DELETE_CELL, id, prop, null, value, row, false);
	}

}
