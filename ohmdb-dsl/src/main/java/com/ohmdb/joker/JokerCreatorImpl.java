package com.ohmdb.joker;

/*
 * #%L
 * ohmdb-dsl
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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import com.ohmdb.abstracts.JokerCreator;
import com.ohmdb.abstracts.Prop;
import com.ohmdb.util.Errors;
import com.ohmdb.util.U;

public class JokerCreatorImpl implements JokerCreator {

	// TODO put code into PropertyInfo!

	private byte n;

	private String trueName;
	private String falseName;

	private final String[] numNames = new String[256];

	private final List<Object> instances = new LinkedList<Object>();
	private final List<String> names = new ArrayList<String>();

	@Override
	public void encode(Object joker, Prop prop) {
		if (!isEnum(prop)) {
			Object value = getCode(prop);
			prop.set(joker, value);
		}
	}

	private Object getCode(Prop prop) {
		if (isEnum(prop)) {
			throw Errors.rte("Encoding for enumeration types is not yet supported!");
		}

		switch (U.kindOf(prop.getType())) {
		case BOOLEAN:
			return trueFalse(prop);
		case BYTE:
			return num(prop);
		case CHAR:
			return (char) num(prop);
		case SHORT:
			return new Short(num(prop));
		case INT:
			return new Integer(num(prop));
		case LONG:
			return new Long(num(prop));
		case FLOAT:
			return new Float(num(prop));
		case DOUBLE:
			return new Double(num(prop));
		case BOOLEAN_OBJ:
			return instance(prop, Boolean.class, true);
		case BYTE_OBJ:
			return num(prop);
		case CHAR_OBJ:
			return (char) num(prop);
		case SHORT_OBJ:
			return new Short(num(prop));
		case INT_OBJ:
			return new Integer(num(prop));
		case LONG_OBJ:
			return new Long(num(prop));
		case FLOAT_OBJ:
			return new Float(num(prop));
		case DOUBLE_OBJ:
			return new Double(num(prop));
		case STRING:
			return "" + num(prop);
		case DATE:
			return instance(prop, Date.class);
		case OBJECT:
			return instance(prop, Object.class);
		default:
			throw Errors.notExpected();
		}
	}

	private Object trueFalse(Prop prop) {
		if (trueName == null) {
			trueName = prop.getName();
			return true;
		} else {
			if (falseName == null) {
				falseName = prop.getName();
				return false;
			} else {
				throw new PropertyCodeException("Maximum 2 primitive 'boolean' properties can be supported!");
			}
		}
	}

	private Object instance(Prop prop, Class<?> clazz, Object... args) {
		Constructor<?>[] constructors = clazz.getConstructors();
		for (Constructor<?> constructor : constructors) {
			if (constructor.getParameterTypes().length == args.length) {
				try {
					Object obj = constructor.newInstance(args);
					instances.add(obj);
					names.add(prop.getName());
					return obj;
				} catch (Exception e) {
					throw Errors.rte(e);
				}
			}
		}
		return constructors;
	}

	private byte num(Prop prop) {
		if (n > 255) {
			throw new PropertyCodeException("Maximum 256 properties/columns are supported!");
		}
		numNames[n] = prop.getName();
		return n++;
	}

	@Override
	public String decode(Object code) {
		if (code instanceof Number) {
			Number number = (Number) code;
			return getN(code, number.longValue());
		} else if (code instanceof Character) {
			Character c = (Character) code;
			return getN(code, c.charValue());
		} else if (code instanceof String) {
			String s = (String) code;
			try {
				return getN(code, Integer.valueOf(s));
			} catch (NumberFormatException e) {
				throw new PropertyCodeException("Invalid property code: " + code);
			}
		} else {
			int i = 0;
			for (Object obj : instances) {
				if (obj == code) {
					return names.get(i);
				}
				i++;
			}
			if (code instanceof Boolean) {
				Boolean b = (Boolean) code;
				String name = b ? trueName : falseName;
				if (name == null) {
					throw new PropertyCodeException("Invalid property code: " + code);
				}
				return name;
			}
		}
		throw new PropertyCodeException("Invalid property code: " + code);
	}

	private String getN(Object code, long index) {
		if (index < 0 || index > 255) {
			throw new PropertyCodeException("Invalid property code: " + code);
		}
		return numNames[(int) index];
	}

	public boolean isEnum(Prop prop) {
		return Enum.class.isAssignableFrom(prop.getType());
	}

}
