package com.ohmdb.bean;

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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import com.ohmdb.abstracts.Column;
import com.ohmdb.abstracts.ComplexIndex;
import com.ohmdb.abstracts.Index;
import com.ohmdb.abstracts.Prop;
import com.ohmdb.api.Transformer;
import com.ohmdb.util.Errors;
import com.ohmdb.util.TypeKind;
import com.ohmdb.util.U;

public class PropertyInfo implements Prop {

	private String name;

	private Field field;

	private Method getter;

	private Method setter;

	private Column column;

	private Index index;

	private Transformer<Object> transformer;

	private Class<?> type;

	private TypeKind kind;

	private Set<ComplexIndex<?>> complexIndices = new HashSet<ComplexIndex<?>>();

	public void setGetter(Method getter) {
		this.getter = getter;
	}

	public void setSetter(Method setter) {
		this.setter = setter;
	}

	public Method getGetter() {
		return getter;
	}

	public Method getSetter() {
		return setter;
	}

	public Field getField() {
		return field;
	}

	public void setField(Field field) {
		this.field = field;
	}

	public Column getColumn() {
		return column;
	}

	public void setColumn(Column column) {
		this.column = column;
	}

	@Override
	public String toString() {
		return "PropertyInfo [field=" + (field != null ? field.getName() : null) + ", getter="
				+ (getter != null ? getter.getName() : null) + ", setter=" + (setter != null ? setter.getName() : null)
				+ "]";
	}

	public Index getIndex() {
		return index;
	}

	public void setIndex(Index index) {
		this.index = index;
	}

	@Override
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public Object get(Object obj) {
		try {
			if (field != null) {
				return field.get(obj);
			} else {
				return getter.invoke(obj);
			}
		} catch (Exception e) {
			throw Errors.rte(e);
		}
	}

	public void set(Object obj, Object value) {
		try {
			if (field != null) {
				field.set(obj, value);
			} else {
				setter.invoke(obj, value);
			}
		} catch (Exception e) {
			throw Errors.rte(e);
		}
	}

	@Override
	public Class<?> getType() {
		if (type == null) {
			// TODO: improve inference from getter and setter
			type = field != null ? field.getType() : getter.getReturnType();
		}
		return type;
	}

	public TypeKind getKind() {
		if (kind == null) {
			kind = U.kindOf(getType());
		}

		return kind;
	}

	public Transformer<Object> getTransformer() {
		return transformer;
	}

	public void setTransformer(Transformer<Object> transformer) {
		this.transformer = transformer;
	}

	public void addComplexIndex(ComplexIndex<?> index) {
		complexIndices.add(index);
	}

	public void appendComplexIndices(ComplexIndex<?>[] indices) {
		for (ComplexIndex<?> indx : complexIndices) {
			appendIndex(indices, indx);
		}
	}

	private void appendIndex(ComplexIndex<?>[] indices, ComplexIndex<?> indx) {
		for (int i = 0; i < indices.length; i++) {
			if (indices[i] == null) {
				indices[i] = indx;
				return;
			}
			if (indices[i] == indx) {
				return;
			}
		}

		throw Errors.rte("Too many indices!");
	}

	public boolean isEnum() {
		return Enum.class.isAssignableFrom(getType());
	}

}
