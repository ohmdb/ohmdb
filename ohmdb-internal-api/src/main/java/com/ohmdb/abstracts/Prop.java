package com.ohmdb.abstracts;

public interface Prop {

	String getName();

	Class<?> getType();

	Object get(Object obj);

	void set(Object joker, Object value);

}
