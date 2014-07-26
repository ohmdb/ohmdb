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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class DB {

	private static OhmDB DEFAULT_DB = null;

	public static OhmDB db(String filename) {
		try {
			Class<?> cls = Class.forName("com.ohmdb.factory.OhmDbFactory");

			for (Method method : cls.getDeclaredMethods()) {
				int modif = method.getModifiers();
				String name = method.getName();
				Class<?>[] params = method.getParameterTypes();

				if (java.lang.reflect.Modifier.isStatic(modif) && !name.equals("main") && params.length == 1
						&& params[0].equals(String.class)) {
					Object db;
					try {
						db = method.invoke(null, filename);
					} catch (IllegalAccessException e) {
						throw new RuntimeException("Cannot invoke factory method in com.ohmdb.factory.OhmDbFactory!", e);
					} catch (IllegalArgumentException e) {
						throw new RuntimeException("Cannot invoke factory method in com.ohmdb.factory.OhmDbFactory!", e);
					} catch (InvocationTargetException e) {
						throw new RuntimeException("Cannot initialize database!", e);
					}

					if (db instanceof OhmDB) {
						return (OhmDB) db;
					} else {
						throw new RuntimeException(
								"The factory method in com.ohmdb.factory.OhmDbFactory returned invalid value, expected OhmDB instance!");
					}
				}
			}

			throw new RuntimeException("Cannot find factory method in com.ohmdb.factory.OhmDbFactory!");

		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Cannot find com.ohmdb.factory.OhmDbFactory!");
		} catch (SecurityException e) {
			throw new RuntimeException("Cannot access com.ohmdb.factory.OhmDbFactory!", e);
		}
	}

	public static OhmDB db() {
		return db(null);
	}

	public static long insert(Object entity) {
		return defaultDb().insert(entity);
	}

	public static void update(Object entity) {
		defaultDb().update(entity);
	}

	public static <T> T get(long id) {
		return defaultDb().get(id);
	}

	public static void delete(long id) {
		defaultDb().delete(id);
	}

	public static void shutdown() {
		defaultDb().shutdown();
	}

	public static <T> TriggerCreator<T> before(Class<T> type) {
		return defaultDb().before(type);
	}

	public static <T> TriggerCreator<T> after(Class<T> type) {
		return defaultDb().after(type);
	}

	public static synchronized OhmDB defaultDb() {
		if (DEFAULT_DB == null) {
			DEFAULT_DB = db("ohm.db");
		}
		return DEFAULT_DB;
	}

	public static synchronized void setDefaultDb(OhmDB defaultDb) {
		DEFAULT_DB = defaultDb;
	}

}
