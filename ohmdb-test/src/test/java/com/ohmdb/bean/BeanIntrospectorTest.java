package com.ohmdb.bean;

/*
 * #%L
 * ohmdb-test
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

import java.util.Date;
import java.util.Map;

import org.testng.annotations.Test;

import com.ohmdb.test.TestCommons;

public class BeanIntrospectorTest extends TestCommons {

	static abstract class Bar {
		@SuppressWarnings("unused")
		private String name2;
		public final Integer age2 = 0;
		protected int num2;
		public int n2 = 0;
		public static int s2 = 0;
		Date date2;

		public void setX(Object x) {
		}

		public abstract void setDate(Date date);
	}

	static class Foo extends Bar {
		private String name;
		public final Integer age = 0;
		protected int num;
		public int n = 0;
		public int x = 0;
		// public Integer n3 = 0;
		public static Integer s = 0;
		Date date;

		public void settings(String s) {
		}

		public String getty() {
			return name;
		}

		public Date getDate() {
			return date;
		}

		public synchronized boolean isOk() {
			return false;
		}

		public final void setOk(boolean ok) {
		}

		@Override
		public void setDate(Date date) {
		}
	}

	@Test
	public void chouldGetProperties() throws Exception {
		BeanIntrospector m = new BeanIntrospector();
		
		BeanInfo info = m.describe(Foo.class);
		Map<String, PropertyInfo> props = info.getProperties();
		
		eq(props.size(), 5);
		
		PropertyInfo prop;
		
		prop = props.get("n");
		isntNull(prop);
		isntNull(prop.getField());
		isNull(prop.getSetter());
		isNull(prop.getGetter());
		
		prop = props.get("n2");
		isntNull(prop);
		isntNull(prop.getField());
		isNull(prop.getSetter());
		isNull(prop.getGetter());
		
		prop = props.get("x");
		isntNull(prop);
		isntNull(prop.getField());
		isNull(prop.getSetter());
		isNull(prop.getGetter());
		
		prop = props.get("date");
		isntNull(prop);
		isNull(prop.getField());
		isntNull(prop.getSetter());
		isntNull(prop.getGetter());
		
		prop = props.get("ok");
		isntNull(prop);
		isNull(prop.getField());
		isntNull(prop.getSetter());
		isntNull(prop.getGetter());
	}

}
