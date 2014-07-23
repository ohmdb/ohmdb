package com.ohmdb.filestore;

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

public class OccupiedSpace {

	public final long address;

	public final long remainingLength;

	public OccupiedSpace(long address, long newLength) {
		this.address = address;
		this.remainingLength = newLength;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (address ^ (address >>> 32));
		result = prime * result + (int) (remainingLength ^ (remainingLength >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		OccupiedSpace other = (OccupiedSpace) obj;
		if (address != other.address)
			return false;
		if (remainingLength != other.remainingLength)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "OccupiedSpace [address=" + address + ", remainingLength=" + remainingLength + "]";
	}

}
