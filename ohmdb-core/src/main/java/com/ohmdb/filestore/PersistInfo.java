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

import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

public class PersistInfo {

	private final NavigableMap<Long, VersionInfo> versions = new TreeMap<Long, VersionInfo>();

	private long version = 0;

	public long nextVersion(Set<Long> slots) {
		// the first version MUST be 1
		long newVersion = ++version;
		versions.put(newVersion, new VersionInfo(slots));
		return newVersion;
	}

	public VersionInfo getVersion(long version) {
		return versions.get(version);
	}

	public boolean loadVersion(long loadedVersion, Set<Long> slots) {
		versions.put(loadedVersion, new VersionInfo(slots));
		if (version < loadedVersion) {
			version = loadedVersion;
			return true;
		} else {
			return false;
		}
	}

	public VersionInfo getLatestVersion() {
		return versions.lastEntry().getValue();
	}

	public void removeOldVersions() {
		// System.out.println(versions.size());
		// FIXME implement this
	}

}
