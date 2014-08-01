package com.ohmdb.abstracts;

import java.util.Set;

public interface Zones {

	public abstract Set<Long> occupy(int num);

	public abstract void occupied(long position);

	public abstract void release(long position);

	public abstract void releaseAll(long... positions);

	public abstract void releaseAll(Set<Long> positions);

	public abstract void occupiedAll(Set<Long> positions);

}