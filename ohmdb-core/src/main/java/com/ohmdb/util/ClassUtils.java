package com.ohmdb.util;

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

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ClassUtils {

	private static final Map<Class<?>, Object> SINGLETONS = new HashMap<Class<?>, Object>();

	public static <T> T instantiate(Class<T> clazz) {
		try {
			return clazz.newInstance();
		} catch (Exception e) {
			throw Errors.rte("Cannot instantiate class!", e);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> T singleton(Class<T> clazz) {
		Object instance = SINGLETONS.get(clazz);

		if (instance == null) {
			instance = instantiate(clazz);
			SINGLETONS.put(clazz, instance);
		}

		return (T) instance;
	}

	public static void setFieldValue(Object instance, String fieldName, Object value) {
		try {
			for (Class<?> c = instance.getClass(); c != Object.class; c = c.getSuperclass()) {
				try {
					Field field = c.getDeclaredField(fieldName);
					field.setAccessible(true);
					field.set(instance, value);
					field.setAccessible(false);
					return;
				} catch (NoSuchFieldException e) {
					// keep searching the filed in the super-class...
				}
			}
		} catch (Exception e) {
			throw Errors.rte("Cannot set field value!", e);
		}

		throw Errors.rte("Cannot find the field '%s' in the class '%s'", fieldName, instance.getClass());
	}

	public static void setFieldValue(Field field, Object instance, Object value) {
		try {
			field.setAccessible(true);
			field.set(instance, value);
			field.setAccessible(false);
		} catch (Exception e) {
			throw Errors.rte("Cannot set field value!", e);
		}
	}

	public static Object getFieldValue(Object instance, String fieldName) {
		try {
			for (Class<?> c = instance.getClass(); c != Object.class; c = c.getSuperclass()) {
				try {
					Field field = c.getDeclaredField(fieldName);
					return getFieldValue(field, instance);
				} catch (NoSuchFieldException e) {
					// keep searching the filed in the super-class...
				}
			}
		} catch (Exception e) {
			throw Errors.rte("Cannot get field value!", e);
		}

		throw Errors.rte("Cannot find the field '%s' in the class '%s'", fieldName, instance.getClass());
	}

	public static Object getFieldValue(Field field, Object instance) {
		try {
			field.setAccessible(true);
			Object value = field.get(instance);
			field.setAccessible(false);

			return value;
		} catch (Exception e) {
			throw Errors.rte("Cannot get field value!", e);
		}
	}

	public static List<Field> getFields(Class<?> clazz) {
		List<Field> allFields = new ArrayList<Field>();

		try {
			for (Class<?> c = clazz; c != Object.class; c = c.getSuperclass()) {
				Field[] fields = c.getDeclaredFields();
				for (Field field : fields) {
					allFields.add(field);
				}
			}

		} catch (Exception e) {
			throw Errors.rte("Cannot instantiate class!", e);
		}

		return allFields;
	}

	public static List<Field> getFieldsAnnotated(Class<?> clazz, Class<? extends Annotation> annotation) {
		List<Field> allFields = new ArrayList<Field>();

		try {
			for (Class<?> c = clazz; c != Object.class; c = c.getSuperclass()) {
				Field[] fields = c.getDeclaredFields();
				for (Field field : fields) {
					if (field.isAnnotationPresent(annotation)) {
						allFields.add(field);
					}
				}
			}

		} catch (Exception e) {
			throw Errors.rte("Cannot instantiate class!", e);
		}

		return allFields;
	}

	public static Method getMethod(Class<?> clazz, String name, Class<?>... parameterTypes) {
		try {
			return clazz.getMethod(name, parameterTypes);
		} catch (NoSuchMethodException e) {
			throw Errors.rte("Cannot find method: %s", e, name);
		} catch (SecurityException e) {
			throw Errors.rte("Cannot access method: %s", e, name);
		}
	}

	public static Method findMethod(Class<?> clazz, String name, Class<?>... parameterTypes) {
		try {
			return clazz.getMethod(name, parameterTypes);
		} catch (NoSuchMethodException e) {
			return null;
		} catch (SecurityException e) {
			return null;
		}
	}

	public static Object invokeStatic(Method m, Object... args) {
		try {
			return m.invoke(null, args);
		} catch (IllegalAccessException e) {
			throw Errors.rte("Cannot statically invoke method '%s' with args: %s", e, m.getName(),
					Arrays.toString(args));
		} catch (IllegalArgumentException e) {
			throw Errors.rte("Cannot statically invoke method '%s' with args: %s", e, m.getName(),
					Arrays.toString(args));
		} catch (InvocationTargetException e) {
			throw Errors.rte("Cannot statically invoke method '%s' with args: %s", e, m.getName(),
					Arrays.toString(args));
		}
	}

	public static Object invoke(Method m, Object target, Object... args) {
		boolean accessible = m.isAccessible();
		try {
			m.setAccessible(true);
			return m.invoke(target, args);
		} catch (Exception e) {
			throw Errors.rte("Cannot invoke method '%s' with args: %s", e, m.getName(), Arrays.toString(args));
		} finally {
			m.setAccessible(accessible);
		}
	}

	public static List<Class<?>> getImplementedInterfaces(Class<?> clazz) {
		try {
			List<Class<?>> interfaces = new LinkedList<Class<?>>();

			for (Class<?> c = clazz; c != Object.class; c = c.getSuperclass()) {
				for (Class<?> interf : c.getInterfaces()) {
					interfaces.add(interf);
				}
			}

			return interfaces;
		} catch (Exception e) {
			throw Errors.rte("Cannot retrieve implemented interfaces!", e);
		}
	}

	public static <T> Constructor<T> getConstructor(Class<T> clazz, Class<?>... paramTypes) {
		try {
			return (Constructor<T>) clazz.getConstructor(paramTypes);
		} catch (Exception e) {
			throw Errors.rte("Cannot find the constructor for %s with param types: %s", e, clazz,
					Arrays.toString(paramTypes));
		}
	}

	public static boolean annotatedMethod(Object instance, String methodName, Class<Annotation> annotation) {
		try {
			Method method = instance.getClass().getMethod(methodName);
			return method.getAnnotation(annotation) != null;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
