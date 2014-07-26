package com.ohmdb.dsl.impl;

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

import com.ohmdb.abstracts.JokerCreator;
import com.ohmdb.abstracts.Updaters;

public abstract class AbstractUpdaters<E> implements Updaters<E> {

	protected final JokerCreator jokerator;

	public AbstractUpdaters(JokerCreator jokerator) {
		this.jokerator = jokerator;
	}

	//
	// @Override
	// public UpdateProp<E> set(byte property, byte value) {
	// return new UpdatePropImpl<E>(jokerator, prop(property), value);
	// }
	//
	// @Override
	// public UpdateProp<E> set(short property, short value) {
	// return new UpdatePropImpl<E>(jokerator, prop(property), value);
	// }
	//
	// @Override
	// public UpdateProp<E> set(int property, int value) {
	// return new UpdatePropImpl<E>(jokerator, prop(property), value);
	// }
	//
	// @Override
	// public UpdateProp<E> set(long property, long value) {
	// return new UpdatePropImpl<E>(jokerator, prop(property), value);
	// }
	//
	// @Override
	// public UpdateProp<E> set(float property, float value) {
	// return new UpdatePropImpl<E>(jokerator, prop(property), value);
	// }
	//
	// @Override
	// public UpdateProp<E> set(double property, double value) {
	// return new UpdatePropImpl<E>(jokerator, prop(property), value);
	// }
	//
	// @Override
	// public UpdateProp<E> set(boolean property, boolean value) {
	// return new UpdatePropImpl<E>(jokerator, prop(property), value);
	// }
	//
	// @Override
	// public UpdateProp<E> set(String property, String value) {
	// return new UpdatePropImpl<E>(jokerator, prop(property), value);
	// }
	//
	// @Override
	// public UpdateProp<E> set(Date property, Date value) {
	// return new UpdatePropImpl<E>(jokerator, prop(property), value);
	// }
	//
	// @Override
	// public UpdateProp<E> setProp(String propertyName, byte value) {
	// return new UpdatePropImpl<E>(jokerator, propertyName, value);
	// }
	//
	// @Override
	// public UpdateProp<E> setProp(String propertyName, short value) {
	// return new UpdatePropImpl<E>(jokerator, propertyName, value);
	// }
	//
	// @Override
	// public UpdateProp<E> setProp(String propertyName, int value) {
	// return new UpdatePropImpl<E>(jokerator, propertyName, value);
	// }
	//
	// @Override
	// public UpdateProp<E> setProp(String propertyName, long value) {
	// return new UpdatePropImpl<E>(jokerator, propertyName, value);
	// }
	//
	// @Override
	// public UpdateProp<E> setProp(String propertyName, float value) {
	// return new UpdatePropImpl<E>(jokerator, propertyName, value);
	// }
	//
	// @Override
	// public UpdateProp<E> setProp(String propertyName, double value) {
	// return new UpdatePropImpl<E>(jokerator, propertyName, value);
	// }
	//
	// @Override
	// public UpdateProp<E> setProp(String propertyName, boolean value) {
	// return new UpdatePropImpl<E>(jokerator, propertyName, value);
	// }
	//
	// @Override
	// public UpdateProp<E> setProp(String propertyName, String value) {
	// return new UpdatePropImpl<E>(jokerator, propertyName, value);
	// }
	//
	// @Override
	// public UpdateProp<E> setProp(String propertyName, Date value) {
	// return new UpdatePropImpl<E>(jokerator, propertyName, value);
	// }

	protected String prop(Object code) {
		return jokerator.decode(code);
	}

}
