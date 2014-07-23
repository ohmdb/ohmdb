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
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.ohmdb.util.Errors;

public class BeanIntrospector {

	private final Map<Class<?>, BeanInfo> cache = new HashMap<Class<?>, BeanInfo>();

	public BeanInfo describe(Class<?> clazz) {
		BeanInfo info = cache.get(clazz);

		if (info == null) {

			info = new BeanInfo();

			try {
				for (Class<?> c = clazz; c != Object.class && c != null; c = c.getSuperclass()) {
					Method[] methods = c.getDeclaredMethods();
					for (Method method : methods) {
						int modif = method.getModifiers();
						if ((modif & Modifier.PUBLIC) > 0 && (modif & Modifier.STATIC) == 0
								&& (modif & Modifier.ABSTRACT) == 0) {
							String name = method.getName();
							if (name.matches("^(get|set|is)[A-Z].*")) {

								String fieldName;
								if (name.startsWith("is")) {
									fieldName = name.substring(2, 3).toLowerCase() + name.substring(3);
								} else {
									fieldName = name.substring(3, 4).toLowerCase() + name.substring(4);
								}

								PropertyInfo propInfo = propInfo(info, fieldName);
								if (name.startsWith("set")) {
									propInfo.setSetter(method);
									method.setAccessible(true);
								} else {
									propInfo.setGetter(method);
									method.setAccessible(true);
								}
							}
						}
					}

					for (Iterator<Entry<String, PropertyInfo>> it = info.getProperties().entrySet().iterator(); it
							.hasNext();) {
						Entry<String, PropertyInfo> entry = (Entry<String, PropertyInfo>) it.next();
						PropertyInfo minfo = entry.getValue();
						if (minfo.getGetter() == null || minfo.getSetter() == null) {
							it.remove();
						}
					}
				}

				for (Class<?> c = clazz; c != Object.class && c != null; c = c.getSuperclass()) {
					Field[] fields = c.getDeclaredFields();
					for (Field field : fields) {
						int modif = field.getModifiers();
						if ((modif & Modifier.PUBLIC) > 0 && (modif & Modifier.FINAL) == 0
								&& (modif & Modifier.STATIC) == 0) {
							String fieldName = field.getName();
							PropertyInfo propInfo = propInfo(info, fieldName);
							if (propInfo.getGetter() == null && propInfo.getSetter() == null) {
								field.setAccessible(true);
								propInfo.setField(field);
							}
						}
					}
				}

			} catch (Exception e) {
				throw Errors.rte(e);
			}

			cache.put(clazz, info);
		}

		return info;
	}

	private PropertyInfo propInfo(BeanInfo info, String fieldName) {
		if (fieldName.equals("id")) {
			PropertyInfo idtor = info.getIdtor();

			if (idtor == null) {
				idtor = new PropertyInfo();
				info.setIdtor(idtor);
			}

			return idtor;
		} else {
			return info.getInfo(fieldName);
		}
	}

}
