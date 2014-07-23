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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.ohmdb.abstracts.IdColl;
import com.ohmdb.abstracts.LockManager;
import com.ohmdb.abstracts.RWRelation;
import com.ohmdb.api.Table;
import com.ohmdb.api.Transaction;
import com.ohmdb.filestore.DataStore;
import com.ohmdb.filestore.DatastoreTransaction;
import com.ohmdb.impl.OhmDBStats;
import com.ohmdb.numbers.Numbers;
import com.ohmdb.numbers.Nums;
import com.ohmdb.transaction.TransactionInternals;
import com.ohmdb.transaction.Transactor;
import com.ohmdb.util.Check;
import com.ohmdb.util.Errors;

@SuppressWarnings({ "serial", "unused" })
public class RelationImpl<FROM, TO> extends AbstractRelation<FROM, TO> {

	private final SortedMap<Long, Numbers> left = new TreeMap<Long, Numbers>();
	private final String name;
	private final SortedMap<Long, Numbers> right = new TreeMap<Long, Numbers>();

	private final DataStore store;
	private final IdColl ids;
	private final Transactor transactor;
	private final LockManager locker;
	private final OhmDBStats stats;

	private final List<RelationChange> changelog = new ArrayList<RelationChange>(10000);

	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	private final Table<FROM> from;

	private final Table<TO> to;

	private boolean symmetric;

	private boolean manyFroms = true;

	private boolean manyTos = true;

	private boolean hasKind;

	public RelationImpl(Table<FROM> from, String name, Table<TO> to, DataStore store, IdColl ids,
			Transactor transactor, LockManager lockManager, OhmDBStats stats, boolean symmetric, boolean manyFroms,
			boolean manyTos) {
		Check.arg(name != null, "The relation name must be specified!");

		this.from = from;
		this.to = to;
		this.name = name;
		this.store = store;
		this.ids = ids;
		this.transactor = transactor;
		this.locker = lockManager;
		this.stats = stats;
		this.symmetric = symmetric;
		this.manyFroms = manyFroms;
		this.manyTos = manyTos;

		this.hasKind = true;
	}

	public RelationImpl(Table<FROM> from, String name, Table<TO> to, DataStore store, IdColl ids,
			Transactor transactor, LockManager lockManager, OhmDBStats stats) {
		Check.arg(name != null, "The relation name must be specified!");

		this.from = from;
		this.to = to;
		this.name = name;
		this.store = store;
		this.ids = ids;
		this.transactor = transactor;
		this.locker = lockManager;
		this.stats = stats;

		this.hasKind = false;
	}

	private boolean link_(long from, long to) {
		// nice("LINK " + from + ":" + to + " ::: " + name + " : "
		// + trees.str(left));

		Numbers leftLinks = leftNums(from);

		if (leftLinks.contains(to)) {
			return false;
		}

		// nice("LINK2 " + from + ":" + to + " ::: " + name +
		// " : " + trees.str(left));

		Check.state(manyTos || leftLinks.size() == 0, "The ?-to-one relation %s already has link from %s", name, from);

		leftLinks = leftLinks.with(to);
		left.put(from, leftLinks);

		Numbers rightLinks = rightNums(to);

		if (!symmetric) {
			assert !rightLinks.contains(from);
		}

		Check.state(manyFroms || rightLinks.size() == 0, "The one-to-? relation %s already has link to %s", name, to);

		rightLinks = rightLinks.with(from);
		rightish().put(to, rightLinks);

		return true;
	}

	private boolean delink_(long from, long to) {
		// nice("DELINK " + from + ":" + to + " ::: " + name +
		// " : " + trees.str(left));
		Numbers leftLinks = leftNums(from);

		if (!leftLinks.contains(to)) {
			return false;
		}

		// nice("DELINK2 " + from + ":" + to + " ::: " + name +
		// " : " + trees.str(left));

		leftLinks = leftLinks.without(to);

		if (leftLinks.size() > 0) {
			left.put(from, leftLinks);
		} else {
			left.remove(from);
		}

		Numbers rightLinks = rightNums(to);

		if (!symmetric) {
			assert rightLinks.contains(from);
		}

		rightLinks = rightLinks.without(from);

		if (rightLinks.size() > 0) {
			rightish().put(to, rightLinks);
		} else {
			rightish().remove(to);
		}

		return true;
	}

	@Override
	public boolean link(long from, long to) {
		try {
			// WRITE LOCK
			locker.relationWriteLock(this);

			nice("START LINKING " + name + ": " + from + " " + to);

			// RUN INSIDE TRANSACTION
			DatastoreTransaction tx = getTransaction();

			boolean changed = linkInTx(tx, from, to);

			// FINISH INSIDE TRANSACTION
			finishTransaction(tx);

			nice("END LINKING " + name + ": " + from + " " + to);
			return changed;

		} finally {
			// WRITE UNLOCK
			locker.relationWriteUnlock(this);
		}
	}

	private boolean linkInTx(DatastoreTransaction tx, long from, long to) {
		// INFORM INSIDER
		insider.linking(name, from, to);

		// MAYBE CHANGE #1
		boolean changed = link_(from, to);

		if (changed) {
			tx.changed(from);
			tx.changed(to);
			changelog.add(RelationChange.link(from, to)); // CHANGELOG
		}

		// INFORM INSIDER
		insider.linked(name, from, to);

		return changed;
	}

	@Override
	public boolean delink(long from, long to) {

		try {
			// WRITE LOCK
			locker.relationWriteLock(this);

			// nice("\nSTART DELINKING " + name + ": " + from + " " + to);

			// RUN INSIDE TRANSACTION
			DatastoreTransaction tx = getTransaction();

			boolean changed = delinkInTx(tx, from, to);

			// FINISH INSIDE TRANSACTION
			finishTransaction(tx);

			return changed;
			// nice("END UNLINKING " + name + ": " + from + " " + to);

		} finally {
			// WRITE UNLOCK
			locker.relationWriteUnlock(this);
		}

	}

	private boolean delinkInTx(DatastoreTransaction tx, long from, long to) {
		// INFORM INSIDER
		insider.delinking(name, from, to);

		// MAYBE CHANGE #1
		boolean changed = delink_(from, to);

		if (changed) {
			tx.changed(from);
			tx.changed(to);
			changelog.add(RelationChange.delink(from, to)); // CHANGELOG
		}

		// INFORM INSIDER
		insider.delinked(name, from, to);

		return changed;
	}

	@Override
	public void deleteFrom(long id) {

		try {
			// WRITE LOCK
			locker.relationWriteLock(this);

			// nice("\nSTART DELETE FROM " + name + ": " + id);

			// RUN INSIDE TRANSACTION
			DatastoreTransaction tx = getTransaction();

			// INFORM INSIDER
			insider.deletingLinksFrom(name, id);

			// DELEGATE TO DELETE LINKS FROM ID INSIDE TRANSACTION
			deleteFromInTx(id, tx);

			// INFORM INSIDER
			insider.deletedLinksFrom(name, id);

			// FINISH INSIDE TRANSACTION
			finishTransaction(tx);

			// nice("\nEND DELETE FROM " + name + ": " + id);

		} finally {
			// WRITE UNLOCK
			locker.relationWriteUnlock(this);
		}

	}

	@Override
	public void deleteTo(long id) {

		try {
			// WRITE LOCK
			locker.relationWriteLock(this);

			// nice("\nSTART DELETE TO " + name + ": " + id);

			// RUN INSIDE TRANSACTION
			DatastoreTransaction tx = getTransaction();

			// INFORM INSIDER
			insider.deletingLinksTo(name, id);

			// DELEGATE TO DELETE LINKS TO ID INSIDE TRANSACTION
			deleteToInTx(id, tx);

			// INFORM INSIDER
			insider.deletedLinksTo(name, id);

			// FINISH INSIDE TRANSACTION
			finishTransaction(tx);

			// nice("\nEND DELETE TO " + name + ": " + id);

		} finally {
			// WRITE UNLOCK
			locker.relationWriteUnlock(this);
		}

	}

	@Override
	public void deleteFromInTx(long fromId, DatastoreTransaction tx) {
		Numbers links = leftNums(fromId);

		for (long toId : links.toArray()) {
			delinkInTx(tx, fromId, toId); // GROUP OF CHANGES
		}
	}

	@Override
	public void deleteToInTx(long toId, DatastoreTransaction tx) {
		Numbers links = rightNums(toId);

		for (long fromId : links.toArray()) {
			delinkInTx(tx, fromId, toId); // GROUP OF CHANGES
		}
	}

	@Override
	public void fill(long fromId, long toId) {
		// nice("@@@ FILL RELATION " + name + ": " + fromId + " => " + toId);
		link_(fromId, toId);
	}

	@Override
	public void clear() {
		try {
			// WRITE LOCK
			locker.relationWriteLock(this);

			// RUN INSIDE TRANSACTION
			DatastoreTransaction tx = getTransaction();

			// INFORM INSIDER

			clearInTx(tx);

			// INFORM INSIDER

			// FINISH INSIDE TRANSACTION
			finishTransaction(tx);

		} finally {
			// WRITE UNLOCK
			locker.relationWriteUnlock(this);
		}

	}

	@Override
	public void clearInTx(DatastoreTransaction tx) {
		for (long from : froms().toArray()) {
			deleteFromInTx(from, tx);
		}
	}

	@Override
	public int fromSize() {
		return left.size();
	}

	@Override
	public int toSize() {
		return rightish().size();
	}

	private Numbers leftNums(long id) {
		Numbers nums = left.get(id);
		return nums != null ? nums : Nums.none();
	}

	private Numbers rightNums(long id) {
		Numbers nums = rightish().get(id);
		return nums != null ? nums : Nums.none();
	}

	@Override
	public String toString() {
		return name;
	}

	@Override
	public String info() {
		String fromTo = from + " => " + name + " => " + to;
		// return "=== RELATION " + fromTo + " ===\n" + txt(left, "\n") +
		// "\n ----------\n" + txt(right, "\n") + "\n";
		return "=== RELATION " + fromTo + " (" + fromSize() + ":" + toSize() + ") ===\n" + txt(left, "\n") + "\n===\n";
	}

	private static String txt(Map<Long, Numbers> map, String sep) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;

		for (Entry<Long, Numbers> entry : map.entrySet()) {
			Object key = entry.getKey();
			Numbers arr = entry.getValue();

			String val = arr.toString();

			if (!first) {
				sb.append(sep);
			}
			sb.append(key);
			sb.append(" -> ");
			sb.append(val);

			first = false;
		}

		return sb.toString();
	}

	@Override
	public Numbers linksFrom(long id) {
		try {
			// READ LOCK
			locker.relationReadLock(this);

			Numbers links = left.get(id);
			return links != null ? links : Nums.none();

		} finally {
			// READ UNLOCK
			locker.relationReadUnlock(this);
		}
	}

	@Override
	public Numbers linksTo(long id) {
		try {
			// READ LOCK
			locker.relationReadLock(this);

			Numbers links = rightish().get(id);
			return links != null ? links : Nums.none();

		} finally {
			// READ UNLOCK
			locker.relationReadUnlock(this);
		}
	}

	private DatastoreTransaction getTransaction() {
		Transaction transaction = transactor.getTransaction();

		if (transaction != null) {
			TransactionInternals internals = (TransactionInternals) transaction;
			return internals.getStoreTx();
		} else {
			return store.transaction();
		}
	}

	private void finishTransaction(DatastoreTransaction tx) {
		Transaction transaction = transactor.getTransaction();

		if (transaction == null) {
			try {
				tx.commit();
				commit();
			} catch (Exception e) {
				e.printStackTrace();
				rollback();
			}
		}
	}

	private void nice(String msg) {
		// System.out.println("### " + msg);
	}

	@Override
	public ReentrantReadWriteLock getLock() {
		return lock;
	}

	@Override
	public void commit() {
		changelog.clear();
	}

	@Override
	public void rollback() {
		// nice("********************   ROLLBACK   **********************");

		for (int i = changelog.size() - 1; i >= 0; i--) {
			RelationChange change = changelog.get(i);
			switch (change.type) {
			case LINK:
				revertLink(change.from, change.to);
				break;
			case DELINK:
				revertDelink(change.from, change.to);
				break;
			default:
				throw Errors.notExpected();
			}
		}

		changelog.clear();
	}

	private void revertLink(long from, long to) {
		delink_(from, to);
	}

	private void revertDelink(long from, long to) {
		link_(from, to);
	}

	@Override
	public String name() {
		return name;
	}

	@Override
	public Table<?> from() {
		return from;
	}

	@Override
	public Table<?> to() {
		return to;
	}

	@Override
	public List<long[]> exportFromTo() {
		return export(left);
	}

	@Override
	public List<long[]> exportToFrom() {
		return export(rightish());
	}

	private List<long[]> export(SortedMap<Long, Numbers> map) {
		List<long[]> links = new ArrayList<long[]>();

		for (Entry<Long, Numbers> entry : map.entrySet()) {
			long key = entry.getKey();
			long[] arr = entry.getValue().toArray();

			for (long x : arr) {
				links.add(new long[] { key, x });
			}
		}

		return links;
	}

	@Override
	public RWRelation inverse() {
		return new InverseRelation<TO, FROM>(this);
	}

	@Override
	public boolean hasLink(long from, long to) {
		try {
			// READ LOCK
			locker.relationReadLock(this);

			return leftNums(from).contains(to);

		} finally {
			// READ UNLOCK
			locker.relationReadUnlock(this);
		}
	}

	@Override
	public Numbers froms() {
		return Nums.fromArray(left.keySet().toArray());
	}

	@Override
	public Numbers tos() {
		return Nums.fromArray(rightish().keySet().toArray());
	}

	@Override
	public void kind(boolean symmetric, boolean manyFroms, boolean manyTos) {
		if (hasKind) {
			Check.state(this.symmetric == symmetric);
			Check.state(this.manyFroms == manyFroms);
			Check.state(this.manyTos == manyTos);
		} else {
			this.symmetric = symmetric;
			this.manyFroms = manyFroms;
			this.manyTos = manyTos;

			this.hasKind = true;
		}
	}

	private SortedMap<Long, Numbers> rightish() {
		return symmetric ? left : right;
	}

}
