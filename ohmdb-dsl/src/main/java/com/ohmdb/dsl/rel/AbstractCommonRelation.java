package com.ohmdb.dsl.rel;

/*
 * #%L
 * ohmdb-dsl
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

import com.ohmdb.abstracts.RWRelation;
import com.ohmdb.api.Relation;
import com.ohmdb.api.Table;
import com.ohmdb.util.Check;
import com.ohmdb.util.U;

public abstract class AbstractCommonRelation<FROM, TO> implements Relation<FROM, TO> {

	public final RWRelation rel;

	public AbstractCommonRelation(RWRelation rel) {
		this.rel = rel;
	}

	@Override
	public void link(long fromId, long toId) {
		rel.link(fromId, toId);
	}

	@Override
	public void delink(long fromId, long toId) {
		rel.delink(fromId, toId);
	}

	@Override
	public boolean hasLink(long fromId, long toId) {
		return rel.hasLink(fromId, toId);
	}

	@Override
	public void link(FROM from, TO to) {
		long fromId = U.getId(from);
		long toId = U.getId(to);
		rel.link(fromId, toId);
	}

	@Override
	public void delink(FROM from, TO to) {
		long fromId = U.getId(from);
		long toId = U.getId(to);
		rel.delink(fromId, toId);
	}

	@Override
	public boolean hasLink(FROM from, TO to) {
		long fromId = U.getId(from);
		long toId = U.getId(to);
		return rel.hasLink(fromId, toId);
	}

	@Override
	public void print() {
		System.out.println(rel.info());
	}

	@SuppressWarnings("unchecked")
	private <T> T[] fetch(long[] ids, Table<?> table) {
		return (T[]) table.getAll(ids);
	}

	@SuppressWarnings("unchecked")
	private <T> T fetch(long id, Table<?> table) {
		return (T) (id >= 0 ? table.get(id) : null);
	}

	protected TO[] froms_(FROM from) {
		return fetch(froms_(U.getId(from)), rel.to());
	}

	protected TO from_(FROM from) {
		return fetch(from_(U.getId(from)), rel.to());
	}

	protected long[] froms_(long id) {
		return rel.linksFrom(id).toArray();
	}

	protected long from_(long id) {
		return only(rel.linksFrom(id).toArray());
	}

	protected FROM[] tos_(TO to) {
		return fetch(tos_(U.getId(to)), rel.from());
	}

	protected FROM to_(TO to) {
		return fetch(to_(U.getId(to)), rel.from());
	}

	protected long[] tos_(long id) {
		return rel.linksTo(id).toArray();
	}

	protected long to_(long id) {
		return only(rel.linksTo(id).toArray());
	}

	private long only(long[] array) {
		Check.state(array.length <= 1, "Expected at most 1 element!");

		return array.length == 1 ? array[0] : -1;
	}

	@Override
	public String toString() {
		return rel.info();
	}

}
