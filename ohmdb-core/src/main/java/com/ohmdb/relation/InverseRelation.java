package com.ohmdb.relation;

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

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.ohmdb.abstracts.RWRelation;
import com.ohmdb.abstracts.RelationInternals;
import com.ohmdb.api.Table;
import com.ohmdb.filestore.DatastoreTransaction;
import com.ohmdb.numbers.Numbers;
import com.ohmdb.util.Errors;

@SuppressWarnings({ "serial" })
public class InverseRelation<FROM, TO> extends AbstractRelation<FROM, TO> {

	private final RelationInternals rel;

	public InverseRelation(RelationInternals rel) {
		this.rel = rel;
	}

	@Override
	public RWRelation inverse() {
		return rel;
	}

	@Override
	public boolean link(long fromId, long toId) {
		return rel.link(toId, fromId);
	}

	@Override
	public boolean delink(long fromId, long toId) {
		return rel.delink(toId, fromId);
	}

	@Override
	public void deleteFrom(long fromId) {
		rel.deleteTo(fromId);
	}

	@Override
	public void deleteTo(long toId) {
		rel.deleteFrom(toId);
	}

	@Override
	public void clear() {
		rel.clear();
	}

	@Override
	public Numbers linksFrom(long id) {
		return rel.linksTo(id);
	}

	@Override
	public Numbers linksTo(long id) {
		return rel.linksFrom(id);
	}

	@Override
	public String info() {
		return "INVERSE OF: " + rel.info();
	}

	@Override
	public String name() {
		return rel.name() + "_inversed";
	}

	@Override
	public Table<?> from() {
		return rel.to();
	}

	@Override
	public Table<?> to() {
		return rel.from();
	}

	@Override
	public List<long[]> exportFromTo() {
		return rel.exportToFrom();
	}

	@Override
	public List<long[]> exportToFrom() {
		return rel.exportFromTo();
	}

	@Override
	public void commit() {
		rel.commit();
	}

	@Override
	public void rollback() {
		rel.rollback();
	}

	@Override
	public void fill(long from, long to) {
		rel.fill(to, from);
	}

	@Override
	public int fromSize() {
		return rel.toSize();
	}

	@Override
	public int toSize() {
		return rel.fromSize();
	}

	@Override
	public void clearInTx(DatastoreTransaction tx) {
		rel.clearInTx(tx);
	}

	@Override
	public void deleteFromInTx(long id, DatastoreTransaction tx) {
		rel.deleteToInTx(id, tx);
	}

	@Override
	public void deleteToInTx(long id, DatastoreTransaction tx) {
		rel.deleteFromInTx(id, tx);
	}

	@Override
	public ReentrantReadWriteLock getLock() {
		return rel.getLock();
	}

	@Override
	public boolean hasLink(long from, long to) {
		return rel.hasLink(to, from);
	}

	@Override
	public String toString() {
		return "inversed(" + rel.name() + ")";
	}

	@Override
	public Numbers froms() {
		return rel.tos();
	}

	@Override
	public Numbers tos() {
		return rel.froms();
	}

	@Override
	public void kind(boolean symmetric, boolean manyFroms, boolean manyTos) {
		throw Errors.notExpected();
	}

}
