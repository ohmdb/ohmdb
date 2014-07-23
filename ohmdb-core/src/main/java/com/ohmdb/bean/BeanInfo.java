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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class BeanInfo {

	private Map<String, PropertyInfo> properties = new HashMap<String, PropertyInfo>();

	private PropertyInfo idtor;

	public Map<String, PropertyInfo> getProperties() {
		return properties;
	}

	@Override
	public String toString() {
		return "BeanInfo [methods=" + properties + "]";
	}

	public PropertyInfo getInfo(String fieldName) {
		PropertyInfo propInfo = properties.get(fieldName);

		if (propInfo == null) {
			propInfo = new PropertyInfo();
			propInfo.setName(fieldName);
			properties.put(fieldName, propInfo);
		}

		return propInfo;
	}

	public PropertyInfo[] getProps() {
		Collection<PropertyInfo> props = properties.values();
		return props.toArray(new PropertyInfo[props.size()]);
	}

	public void setIdtor(PropertyInfo idtor) {
		this.idtor = idtor;
	}

	public PropertyInfo getIdtor() {
		return idtor;
	}
}
