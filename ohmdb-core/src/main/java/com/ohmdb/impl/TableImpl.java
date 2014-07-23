package com.ohmdb.impl;

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

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.ohmdb.abstracts.Any;
import com.ohmdb.abstracts.Column;
import com.ohmdb.abstracts.DbInsider;
import com.ohmdb.abstracts.IdAddress;
import com.ohmdb.abstracts.IdColl;
import com.ohmdb.abstracts.Index;
import com.ohmdb.abstracts.LockManager;
import com.ohmdb.abstracts.TableInternals;
import com.ohmdb.api.Criteria;
import com.ohmdb.api.CustomIndex;
import com.ohmdb.api.Ids;
import com.ohmdb.api.Mapper;
import com.ohmdb.api.Op;
import com.ohmdb.api.Parameter;
import com.ohmdb.api.SearchCriteria;
import com.ohmdb.api.SearchCriteriaKind;
import com.ohmdb.api.SearchCriterion;
import com.ohmdb.api.Table;
import com.ohmdb.api.Transaction;
import com.ohmdb.api.Transformer;
import com.ohmdb.api.Trigger;
import com.ohmdb.api.TriggerAction;
import com.ohmdb.api.Visitor;
import com.ohmdb.bean.BeanInfo;
import com.ohmdb.bean.BeanIntrospector;
import com.ohmdb.bean.PropertyInfo;
import com.ohmdb.dsl.criteria.impl.ComplexCriteriaImpl;
import com.ohmdb.dsl.criteria.impl.CriteriaImpl;
import com.ohmdb.exception.InvalidColumnException;
import com.ohmdb.exception.InvalidIdException;
import com.ohmdb.filestore.DataStore;
import com.ohmdb.filestore.DatastoreTransaction;
import com.ohmdb.index.ComplexIndex;
import com.ohmdb.joker.JokerCreator;
import com.ohmdb.numbers.Numbers;
import com.ohmdb.numbers.Nums;
import com.ohmdb.transaction.TransactionInternals;
import com.ohmdb.transaction.Transactor;
import com.ohmdb.util.Check;
import com.ohmdb.util.Errors;
import com.ohmdb.util.U;
import com.ohmdb.util.UTILS;

public class TableImpl<E> implements Table<E>, TableInternals<E> {

	private long deletedCount;
	private int size;
	private int rows;

	private final SortedSet<Long> ids = new TreeSet<Long>();

	private final Queue<Integer> deleted = new LinkedList<Integer>();
	private final List<Integer> currentlyDeleted = new LinkedList<Integer>();

	private final BeanIntrospector introspector = new BeanIntrospector();
	private final JokerCreator jokerator = new JokerCreator();

	private final OhmDBImpl db;

	private final Class<E> clazz;
	private final BeanInfo info;
	private final PropertyInfo idtor;
	private final PropertyInfo[] props;

	private final DataStore store;
	private final IdColl idColl;

	private final Transactor transactor;

	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	private final List<TableChange> changelog = new ArrayList<TableChange>(10000);
	private final OhmDBStats stats;
	private final LockManager locker;

	private DbInsider insider = new IgnorantInsider();
	private final Constructor<E> constr;
	private final E queryHelper;
	private final List<E> records = new ArrayList<E>(10000);

	@SuppressWarnings("unchecked")
	private ComplexIndex<E>[] allComplexIndices = new ComplexIndex[0];

	@SuppressWarnings("unchecked")
	private final ComplexIndex<E>[] tmpIndices = new ComplexIndex[1000];

	@SuppressWarnings("unchecked")
	private Trigger<E>[] beforeUpdate = new Trigger[0];

	@SuppressWarnings("unchecked")
	private Trigger<E>[] afterUpdate = new Trigger[0];

	@SuppressWarnings("unchecked")
	private Trigger<E>[] beforeInsert = new Trigger[0];

	@SuppressWarnings("unchecked")
	private Trigger<E>[] beforeRead = new Trigger[0];

	@SuppressWarnings("unchecked")
	private Trigger<E>[] afterInsert = new Trigger[0];

	@SuppressWarnings("unchecked")
	private Trigger<E>[] beforeDelete = new Trigger[0];

	@SuppressWarnings("unchecked")
	private Trigger<E>[] afterDelete = new Trigger[0];

	@SuppressWarnings("unchecked")
	private Trigger<E>[] afterRead = new Trigger[0];

	public TableImpl(OhmDBImpl db, Class<E> clazz, DataStore store, IdColl ids, Transactor transactor,
			OhmDBStats stats, LockManager lockManager) {
		this.db = db;
		this.idColl = ids;
		this.transactor = transactor;
		this.stats = stats;
		this.locker = lockManager;
		this.info = introspector.describe(clazz);

		this.clazz = clazz;
		this.store = store;
		this.size = 0;
		this.rows = 0;

		this.props = info.getProps();
		this.idtor = info.getIdtor();

		this.constr = findConstructor();
		this.queryHelper = newEntity();

		init();
	}

	private void init() {
		for (PropertyInfo prop : props) {
			// ContainerName names = ContainerName.classAndColumnName(clazz,
			// prop.getName());
			prop.setColumn(createColumn(prop));
		}

		for (PropertyInfo prop : props) {
			jokerator.encode(queryHelper, prop);
		}
	}

	private Column createColumn(PropertyInfo prop) {
		Field field = prop.getField();

		Column column = field != null ? new FieldColumn(field, records) : new PropertyColumn(prop.getGetter(),
				prop.getSetter(), records);
		return column;
	}

	@SuppressWarnings("unchecked")
	private Constructor<E> findConstructor() {
		Constructor<?>[] constructors = clazz.getDeclaredConstructors();
		for (Constructor<?> constructor : constructors) {
			if (constructor.getParameterTypes().length == 0) {
				Constructor<E> cons = (Constructor<E>) constructor;
				cons.setAccessible(true);
				return cons;
			}
		}

		throw Errors.rte("Couldn't find 0-params constructor for class: " + clazz);
	}

	private void doTriggers(Trigger<E>[] triggers, TriggerAction action, long id, E old, E pre) {
		for (int i = 0; i < triggers.length; i++) {
			triggers[i].process(action, id, old, pre);
		}
	}

	private PropertyInfo colInfo(String columnName) {
		return info.getProperties().get(columnName);
	}

	@Override
	public Object read(long id, String col) {
		try {
			// READ LOCK
			locker.tableReadLock(this);

			checkId(id);

			insider.reading(clazz, id, col);

			Object value = readCell(id, col);

			insider.read(clazz, id, col, value);

			return value;

		} finally {
			// READ UNLOCK
			locker.tableReadUnlock(this);
		}
	}

	@Override
	public void clear() {
		try {
			// WRITE LOCK
			locker.tableWriteLock(this);

			// RUN INSIDE TRANSACTION
			DatastoreTransaction tx = getTransaction();

			long[] allIds = ids();
			for (long id : allIds) {
				deleteInTx(tx, id);
			}

			// FINISH INSIDE TRANSACTION
			finishTransaction(tx);

		} finally {
			// WRITE UNLOCK
			locker.tableWriteUnlock(this);
		}
	}

	private E get_(long id) {
		E entity = newEntity();
		get_(id, entity);
		return entity;
	}

	private void get_(long id, E entity) {
		for (PropertyInfo prop : props) {
			Column column = prop.getColumn();
			Object value = column.get(row(id));
			prop.set(entity, value);
		}
		setId(entity, id);
	}

	private void get_(long id, E entity1, E entity2) {
		for (PropertyInfo prop : props) {
			Column column = prop.getColumn();
			Object value = column.get(row(id));
			prop.set(entity1, value);
			prop.set(entity2, value);
		}
		setId(entity1, id);
		setId(entity2, id);
	}

	@SuppressWarnings("unused")
	private void get_(long id, E entity1, E entity2, E entity3) {
		for (PropertyInfo prop : props) {
			Column column = prop.getColumn();
			Object value = column.get(row(id));
			prop.set(entity1, value);
			prop.set(entity2, value);
			prop.set(entity3, value);
		}
		setId(entity1, id);
		setId(entity2, id);
		setId(entity3, id);
	}

	private void setId(E entity, long id) {
		if (idtor != null) {
			idtor.set(entity, id);
		}
	}

	@Override
	public E get(long id) {
		try {
			// READ LOCK
			locker.tableReadLock(this);

			checkId(id);

			insider.getting(clazz, id);

			E entity = newEntity();

			doTriggers(beforeRead, TriggerAction.BEFORE_READ, id, null, entity);

			get_(id, entity);

			doTriggers(afterRead, TriggerAction.AFTER_READ, id, null, entity);

			insider.got(clazz, id, entity);

			return entity;
		} finally {
			// READ UNLOCK
			locker.tableReadUnlock(this);
		}
	}

	@Override
	public void load(long id, E entity) {
		try {
			// READ LOCK
			locker.tableReadLock(this);

			checkId(id);

			insider.getting(clazz, id);

			doTriggers(beforeRead, TriggerAction.BEFORE_READ, id, null, entity);

			get_(id, entity);

			doTriggers(afterRead, TriggerAction.AFTER_READ, id, null, entity);

			insider.got(clazz, id, entity);
		} finally {
			// READ UNLOCK
			locker.tableReadUnlock(this);
		}
	}

	private E newEntity() {
		try {
			return constr.newInstance();
		} catch (Exception e) {
			throw Errors.rte(e);
		}
	}

	private int row(long id) {
		IdAddress addr = idColl.get(id);
		Check.notNull(addr, "address for ID:" + id);

		Check.arg(this.equals(addr.table), "The ID doesn't match for this table!");

		return addr.row;
	}

	@Override
	public long[] find(SearchCriteria criteria) {
		try {
			// READ LOCK
			locker.tableReadLock(this);

			long[] ids = findBy(criteria).toArray();

			return ids;

		} finally {
			// READ UNLOCK
			locker.tableReadUnlock(this);
		}
	}

	private Numbers findBy(SearchCriteria criteria) {
		switch (criteria.kind()) {
		case CRITERION:
			SearchCriterion crit = criteria.criterion();
			Op op = crit.op();
			Object value = crit.value();
			String columnName = crit.columnName();
			CustomIndex<?, ?> indexer = crit.indexer();

			Check.state(U.xor(columnName != null, indexer != null), "invalid search criteria");

			if (value instanceof Parameter) {
				Parameter<?> param = (Parameter<?>) value;
				throw Errors.illegalArgument("Value not specified for parameter: " + param.name());
			}

			if (op == Op.NEQ) {
				SearchCriteria critLT, critGT;

				if (columnName != null) {
					critLT = SearchCriteriaImpl.single(columnName, Op.LT, value);
					critGT = SearchCriteriaImpl.single(columnName, Op.GT, value);
				} else {
					critLT = SearchCriteriaImpl.single(indexer, Op.LT, value);
					critGT = SearchCriteriaImpl.single(indexer, Op.GT, value);
				}

				return findBy(SearchCriteriaImpl.disjunction(critLT, critGT));
			}

			if (columnName != null) {
				Index index = indexOf(columnName);
				Transformer<Object> tr = prop(columnName).getTransformer();
				return index.find(op, tr.transform(value));
			} else {
				if (indexer instanceof IndexerImpl) {
					IndexerImpl<?, ?> ind = (IndexerImpl<?, ?>) indexer;
					Index index = ind.getIndex();
					return index.find(op, value);
				} else {
					throw Errors.notExpected();
				}
			}

		case CONJUNCTION:
		case DISJUNCTION:
			SearchCriteria[] crits = criteria.criteria();
			Numbers[] ids = new Numbers[crits.length];

			for (int i = 0; i < crits.length; i++) {
				ids[i] = findBy(crits[i]);
			}

			return criteria.kind() == SearchCriteriaKind.CONJUNCTION ? Nums.intersectAll(ids) : Nums.unionAll(ids);

		default:
			throw Errors.notExpected();
		}
	}

	private Index indexOf(String column) {
		Index index = colInfo(column).getIndex();

		Check.state(index != null, "The column %s doesn't have index!", column);

		return index;
	}

	@Override
	public long insert(E entity) {
		try {
			// WRITE LOCK
			locker.tableWriteLock(this);

			long id = insertRow(entity);

			return id;

		} finally {
			// WRITE UNLOCK
			locker.tableWriteUnlock(this);
		}
	}

	@Override
	public long insert(Map<String, Object> properties) {
		try {
			// WRITE LOCK
			locker.tableWriteLock(this);

			E entity = newEntity();

			if (properties != null) {
				for (Entry<String, Object> entry : properties.entrySet()) {
					PropertyInfo prop = info.getProperties().get(entry.getKey());
					prop.set(entity, entry.getValue());
				}
			}

			return insertRow(entity);

		} finally {
			// WRITE UNLOCK
			locker.tableWriteUnlock(this);
		}
	}

	private long insertRow(E entity) {
		// RUN INSIDE TRANSACTION
		DatastoreTransaction tx = getTransaction();

		long id = idColl.newId(); // CHANGE #1

		doTriggers(beforeInsert, TriggerAction.BEFORE_INSERT, id, null, entity);

		// INFORM INSIDER
		insider.inserting(clazz, id, entity);

		stats.inserts++;

		Integer deletedRow = deleted.poll(); // CHANGE #2

		boolean reuseRow = deletedRow != null;
		int row = reuseRow ? deletedRow : rows++; // CHANGE #3

		IdAddress addr = new IdAddress(this, row);
		idColl.set(id, addr); // CHANGE #4

		size++; // CHANGE #5

		ids.add(id); // CHANGE #6

		if (reuseRow) {
			records.set(row, newEntity()); // CHANGE #7a
		} else {
			records.add(newEntity()); // CHANGE #7b
		}

		changelog.add(TableChange.insertRow(id, row, reuseRow)); // CHANGELOG

		if (entity != null) {
			for (PropertyInfo prop : props) {
				Object value = prop.get(entity);
				insertCell(tx, prop, id, row, value); // DELEGATE CHANGES
			}
		}

		setId(entity, id);

		doTriggers(afterInsert, TriggerAction.AFTER_INSERT, id, null, entity);

		// INFORM INSIDER
		insider.inserted(clazz, id, entity);

		// FINISH INSIDE TRANSACTION
		finishTransaction(tx);

		return id;
	}

	// records.set(row, newEntity()); // CHANGE #7a
	// records.add(newEntity()); // CHANGE #7b

	private void revertInsertRow(long id, int row, boolean reuseRow) {
		// INFORM INSIDER
		insider.uninserting(clazz, id);

		idColl.cancelId(id); // UNDO CHANGE #1

		if (reuseRow) {
			deleted.add(row); // UNDO CHANGE #2
		} else {
			rows--; // UNDO CHANGE #3
		}

		idColl.delete(id); // UNDO CHANGE #4

		size--; // UNDO CHANGE #5

		ids.remove(id); // UNDO CHANGE #6

		if (reuseRow) {
			// NO NEED TO UNDO CHANGE #7a
		} else {
			records.remove(records.size() - 1); // UNDO CHANGE #7b
		}
	}

	private void insertCell(DatastoreTransaction tx, PropertyInfo prop, long id, int row, Object value) {
		// INFORM INSIDER
		insider.insertingCell(clazz, id, prop.getName(), value);

		Column column = prop.getColumn();
		Index index = prop.getIndex();

		column.set(row, value); // CHANGE #1

		if (index != null) {
			index.add(prop.getTransformer().transform(value), id); // CHANGE #2
		}

		// CHANGELOG
		changelog.add(TableChange.insertCell(prop, id, row, value));

		tx.changed(id); // PERSIST
	}

	private void revertInsertCell(PropertyInfo prop, long id, int row, Object value) {
		// INFORM INSIDER
		insider.uninsertingCell(clazz, id, prop.getName(), value);

		Column column = prop.getColumn();
		Index index = prop.getIndex();

		column.delete(row); // UNDO CHANGE #1

		if (index != null) {
			// UNDO CHANGE #2
			Transformer<Object> tr = prop.getTransformer();
			index.remove(tr.transform(value), id);
		}
	}

	private Object setCell(DatastoreTransaction tx, long id, PropertyInfo prop, Object value,
			ComplexIndex<?>[] complexIndices) {
		int row = row(id);

		Column column = prop.getColumn();
		Index index = prop.getIndex();

		Object oldValue = column.get(row);

		column.set(row, value); // CHANGE #1

		if (index != null) {
			Transformer<Object> tr = prop.getTransformer();
			index.remove(tr.transform(oldValue), id); // CHANGE #2
			index.add(tr.transform(value), id); // CHANGE #3
		}

		prop.appendComplexIndices(complexIndices);

		changelog.add(TableChange.set(id, prop, oldValue, value)); // CHANGELOG

		tx.changed(id); // PERSIST

		return oldValue;
	}

	private void doComplexIndices(ComplexIndex<E>[] complexIndices, E oldEntity, E newEntity, long id) {
		for (int i = 0; i < complexIndices.length; i++) {
			ComplexIndex<E> indx = complexIndices[i];

			if (indx == null) {
				return;
			}

			indx.remove(oldEntity, id);
			indx.add(newEntity, id);
		}
	}

	private void revertSetCell(long id, PropertyInfo prop, Object oldValue, Object value) {
		// INFORM INSIDER
		insider.unchanging(clazz, id, prop.getName(), oldValue, value);

		int row = row(id);

		Column column = prop.getColumn();
		Index index = prop.getIndex();

		column.set(row, oldValue); // UNDO CHANGE #1

		if (index != null) {
			Transformer<Object> tr = prop.getTransformer();
			index.add(tr.transform(oldValue), id); // UNDO CHANGE #2
			index.remove(tr.transform(value), id); // UNDO CHANGE #3
		}
	}

	@Override
	public void delete(long id) {
		try {
			// WRITE LOCK
			locker.tableWriteLock(this);

			checkId(id);

			// RUN INSIDE TRANSACTION
			DatastoreTransaction tx = getTransaction();

			deleteInTx(tx, id);

			// FINISH INSIDE TRANSACTION
			finishTransaction(tx);

		} finally {
			// WRITE UNLOCK
			locker.tableWriteUnlock(this);
		}
	}

	private void deleteInTx(DatastoreTransaction tx, long id) {
		insider.deleting(clazz, id);

		E entity = get_(id);

		doTriggers(beforeDelete, TriggerAction.BEFORE_DELETE, id, entity, null);

		stats.deletes++;

		int row = row(id);

		// FIXME: check - changelog if not deleted rels?

		db.deleteRelsInTx(id, tx); // CHANGE #1

		size--; // CHANGE #2
		deletedCount++; // CHANGE #3
		currentlyDeleted.add(row); // CHANGE #4
		idColl.delete(id); // CHANGE #5
		ids.remove(id); // CHANGE #6

		changelog.add(TableChange.delete(id, row)); // CHANGELOG

		// TODO also delete old (renamed) columns?
		for (PropertyInfo prop : props) {
			deleteCell(tx, id, row, prop);
		}

		doTriggers(afterDelete, TriggerAction.AFTER_DELETE, id, entity, null);

		insider.deleted(clazz, id);
	}

	public void revertDelete(long id, int row) {
		insider.undeleting(clazz, id);

		// CHANGE #1 (deleted relations) is processed by relations

		size++; // UNDO CHANGE #2
		deletedCount--; // UNDO CHANGE #3
		currentlyDeleted.clear(); // UNDO CHANGE #4 (AGGREGATED)

		IdAddress addr = new IdAddress(this, row);
		idColl.set(id, addr); // UNDO CHANGE #5

		ids.add(id); // UNDO CHANGE #6
	}

	private void deleteCell(DatastoreTransaction tx, long id, int row, PropertyInfo prop) {
		insider.deletingCell(clazz, id, prop.getName());

		Column column = prop.getColumn();
		Index index = prop.getIndex();

		Object value = column.delete(row); // CHANGE #1

		if (index != null) {
			Transformer<Object> tr = prop.getTransformer();
			index.remove(tr.transform(value), id); // CHANGE #2
		}

		changelog.add(TableChange.deleteCell(id, prop, row, value)); // CHANGELOG

		tx.delete(id); // PERSIST
	}

	public void revertDeleteCell(long id, int row, PropertyInfo prop, Object value) {
		insider.undeletingCell(clazz, id, prop.getName());

		Column column = prop.getColumn();
		Index index = prop.getIndex();

		column.set(row, value); // UNDO CHANGE #1

		if (index != null) {
			Transformer<Object> tr = prop.getTransformer();
			index.add(tr.transform(value), id); // UNDO CHANGE #2
		}
	}

	@Override
	public void print() {
		try {
			// READ LOCK
			locker.tableReadLock(this);

			System.out.println("*** Table " + name() + " ***");
			for (long id : ids()) {
				E entity = get_(id);
				System.out.println(entity.toString());
			}

		} finally {
			// READ UNLOCK
			locker.tableReadUnlock(this);
		}
	}

	@Override
	public int size() {
		try {
			// READ LOCK
			locker.tableReadLock(this);

			return size;

		} finally {
			// READ UNLOCK
			locker.tableReadUnlock(this);
		}
	}

	@Override
	public void update(long id, E entity) {
		try {
			// WRITE LOCK
			locker.tableWriteLock(this);

			checkId(id);

			// RUN INSIDE TRANSACTION
			DatastoreTransaction tx = getTransaction();

			doUpdate(id, tx, entity);

			// FINISH INSIDE TRANSACTION
			finishTransaction(tx);

		} finally {
			// WRITE UNLOCK
			locker.tableWriteUnlock(this);
		}
	}

	@Override
	public void update(E entity) {
		try {
			// WRITE LOCK
			locker.tableWriteLock(this);

			long id = UTILS.getId(entity);
			checkId(id);

			// RUN INSIDE TRANSACTION
			DatastoreTransaction tx = getTransaction();

			doUpdate(id, tx, entity);

			// FINISH INSIDE TRANSACTION
			finishTransaction(tx);

		} finally {
			// WRITE UNLOCK
			locker.tableWriteUnlock(this);
		}
	}

	@Override
	public void update(long id, Map<String, Object> properties) {
		try {
			// WRITE LOCK
			locker.tableWriteLock(this);

			checkId(id);

			// RUN INSIDE TRANSACTION
			DatastoreTransaction tx = getTransaction();

			E entity = get_(id);

			for (Entry<String, Object> entry : properties.entrySet()) {
				String col = entry.getKey();
				checkColumn(col);
				colInfo(col).set(entity, entry.getValue());
			}

			doUpdate(id, tx, entity);

			// FINISH INSIDE TRANSACTION
			finishTransaction(tx);

		} finally {
			// WRITE UNLOCK
			locker.tableWriteUnlock(this);
		}
	}

	@Override
	public void set(long id, String col, Object value) {
		try {
			// WRITE LOCK
			locker.tableWriteLock(this);

			checkId(id);
			checkColumn(col);

			// RUN INSIDE TRANSACTION
			DatastoreTransaction tx = getTransaction();

			// INFORM INSIDER
			insider.changing(clazz, id, col, value);

			E entity = get_(id);

			colInfo(col).set(entity, value);

			doUpdate(id, tx, entity);

			// INFORM INSIDER
			insider.changed(clazz, id, col, value);

			// FINISH INSIDE TRANSACTION
			finishTransaction(tx);

		} finally {
			// WRITE UNLOCK
			locker.tableWriteUnlock(this);
		}
	}

	private void doUpdate(long id, DatastoreTransaction tx, E entity) {
		E old = newEntity(), old2 = newEntity(), post = newEntity();
		get_(id, old, old2);

		doTriggers(beforeUpdate, TriggerAction.BEFORE_UPDATE, id, old, entity);

		for (int i = 0; i < tmpIndices.length && tmpIndices[i] != null; i++) {
			tmpIndices[i] = null;
		}

		for (PropertyInfo pr : props) {
			Object val = pr.get(entity);
			setCell(tx, id, pr, val, tmpIndices);
		}

		doTriggers(afterUpdate, TriggerAction.AFTER_UPDATE, id, old, entity);

		get_(id, post);

		// old might be changed by triggers, so use old2 as protection
		doComplexIndices(tmpIndices, old2, post, id);
	}

	private Object readCell(long id, String col) {
		int row = row(id);

		Column column = colInfo(col).getColumn();

		return column.get(row);
	}

	long getDeletedCount() {
		return deletedCount;
	}

	long getUnusedCount() {
		return deleted.size() + currentlyDeleted.size();
	}

	long getReuusedCount() {
		return deletedCount - getUnusedCount();
	}

	int mem() {
		return rows;
	}

	void revalidate() {
		// for (PropertyInfo prop : props) {
		// Column column = prop.getColumn();
		// Check.state(rows == column.size(), "Inconsistent rows count");
		// }

		Check.state(deletedCount >= getUnusedCount(), "Inconsistent deleted count");
	}

	@Override
	public E queryHelper() {
		return queryHelper;
	}

	@Override
	public String nameOf(Object property) {
		return jokerator.decode(property);
	}

	@Override
	public void fill(long id, String columnName, Object value) {

		boolean shouldInsert = !idColl.has(id);

		// System.out.println("FILL " + id + " COL=" + columnName + " VAL=" +
		// value + " INS=" + shouldInsert);
		int row;

		if (shouldInsert) {
			idColl.registerId(id);
			row = rows++;
			idColl.set(id, new IdAddress(this, row));
			size++;
			records.add(newEntity());
			ids.add(id);
		} else {
			row = row(id);
		}

		checkId(id);
		checkColumn(columnName);

		PropertyInfo prop = colInfo(columnName);

		Column column = prop.getColumn();
		column.set(row, value);
	}

	@Override
	public ReentrantReadWriteLock getLock() {
		return lock;
	}

	@Override
	public void commit() {
		changelog.clear();
		deleted.addAll(currentlyDeleted);
		currentlyDeleted.clear();
	}

	@Override
	public void rollback() {
		// System.out.println("********************   ROLLBACK   **********************");

		for (int i = changelog.size() - 1; i >= 0; i--) {
			TableChange change = changelog.get(i);
			switch (change.type) {
			case INSERT_ROW:
				revertInsertRow(change.id, change.row, change.reuseRow);
				break;
			case INSERT_CELL:
				revertInsertCell(change.prop, change.id, change.row, change.value);
				break;
			case SET_CELL:
				revertSetCell(change.id, change.prop, change.oldValue, change.value);
				break;
			case DELETE:
				revertDelete(change.id, change.row);
				break;
			case DELETE_CELL:
				revertDeleteCell(change.id, change.row, change.prop, change.value);
				break;
			default:
				throw Errors.notExpected();
			}
		}

		changelog.clear();
		currentlyDeleted.clear();
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
				System.out.println(" === EXCEPTION IN TRANSACTION: ===");
				e.printStackTrace();
				System.out.println(" === ROLLING BACK... ===");
				rollback();
			}
		}
	}

	@Override
	public Class<E> getClazz() {
		return clazz;
	}

	@Override
	public void setInsider(DbInsider insider) {
		this.insider = insider;
	}

	private IdAddress checkId(long id) {
		IdAddress address = idColl.get(id);

		if (address == null || address.table != this) {
			insider.invalidId(clazz, id);
			throw new InvalidIdException(id); // TODO: different exception for
												// wrong table
		}

		return address;
	}

	private boolean validColumn(String column) {
		boolean ok = info.getProperties().containsKey(column);
		if (!ok) {
			insider.invalidColumn(clazz, column);
		}
		return ok;
	}

	private void checkColumn(String column) {
		if (!validColumn(column)) {
			throw new InvalidColumnException(column);
		}
	}

	@Override
	public Any<E> all() {
		return new AnySearch<E>(this);
	}

	@Override
	public long[] ids() {
		return Nums.arrFrom(ids);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void updateObj(Object entity) {
		update((E) entity);
	}

	@Override
	public JokerCreator jokerator() {
		return jokerator;
	}

	@Override
	public Ids<E> withIds(long... ids) {
		return new RecordsImpl<E>(this, ids);
	}

	@SuppressWarnings("unchecked")
	@Override
	public E[] getAll(long... ids) {
		E[] arr = (E[]) Array.newInstance(clazz, ids.length);

		for (int i = 0; i < ids.length; i++) {
			arr[i] = get(ids[i]);
		}

		return arr;
	}

	@Override
	public String toString() {
		return "TABLE<" + name() + ">";
	}

	@Override
	public String name() {
		return clazz.getSimpleName();
	}

	@Override
	public <T> Criteria<E, T> where(T property) {
		return new CriteriaImpl<E, T>(this, jokerator.decode(property));
	}

	@Override
	public <T> Criteria<E, T> where(String propertyName, Class<T> type) {
		return new CriteriaImpl<E, T>(this, propertyName);
	}

	@Override
	public <T> Criteria<E, T> where(CustomIndex<E, T> indexer) {
		return new ComplexCriteriaImpl<E, T>(this, indexer);
	}

	@Override
	public <T> void createIndexOn(T property) {
		createIndexOnNamed(jokerator.decode(property));
	}

	@Override
	public void createIndexOnNamed(String propertyName) {
		createIndexOnNamed(propertyName, new Transformer<Object>() {
			@Override
			public Object transform(Object value) {
				return value;
			}
		});
	}

	@Override
	public <T> void createIndexOn(T property, Transformer<T> transformer) {
		createIndexOnNamed(jokerator.decode(property), transformer);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> void createIndexOnNamed(String propertyName, Transformer<T> transformer) {
		try {
			// WRITE LOCK
			locker.tableWriteLock(this);

			PropertyInfo prop = prop(propertyName);

			Index oldIndex = prop.getIndex();
			prop.setIndex(null); // temporary set to null

			if (oldIndex != null) {
				oldIndex.dispose();
			}

			Transformer<Object> tr = (Transformer<Object>) transformer;
			prop.setTransformer(tr);

			Index index = makeIndex();
			prop.setIndex(index);

			long[] idss = ids();

			for (long id : idss) {
				Object value = read(id, propertyName);
				value = tr.transform(value);
				index.add(value, id);
			}

		} finally {
			// WRITE UNLOCK
			locker.tableWriteUnlock(this);
		}
	}

	@Override
	public void forEach(Visitor<E> visitor) {
		forEach(ids(), visitor);
	}

	@Override
	public void forEach(long[] ids, Visitor<E> visitor) {
		E entity = newEntity();

		for (long id : ids) {
			get_(id, entity);
			if (visitor.visit(entity)) {
				update(entity);
			}
		}
	}

	private PropertyInfo prop(String propertyName) {
		PropertyInfo prop = info.getProperties().get(propertyName);
		Check.state(prop != null, "Unknown property: %s", propertyName);
		return prop;
	}

	@Override
	public <T> CustomIndex<E, T> index(Mapper<E, T> mapper, Object... properties) {
		try {
			// WRITE LOCK
			locker.tableWriteLock(this);

			return complexIndex(properties, mapper);

		} finally {
			// WRITE UNLOCK
			locker.tableWriteUnlock(this);
		}
	}

	@Override
	public <T> CustomIndex<E, T> multiIndex(Mapper<E, T[]> mapper, Object... properties) {
		try {
			// WRITE LOCK
			locker.tableWriteLock(this);

			return complexIndex(properties, mapper);

		} finally {
			// WRITE UNLOCK
			locker.tableWriteUnlock(this);
		}
	}

	@SuppressWarnings("unchecked")
	private <T> IndexerImpl<E, T> complexIndex(Object[] columns, final Mapper<E, ?> mapper) {
		Index index = makeIndex();
		ComplexIndex<E> complexIndex = new ComplexIndexImpl<E>((Mapper<E, Object>) mapper, index);

		for (Object col : columns) {
			String colName = jokerator.decode(col);
			prop(colName).addComplexIndex(complexIndex);
		}

		allComplexIndices = U.expand(allComplexIndices, complexIndex);

		E entity = newEntity();

		for (long id : ids()) {
			get_(id, entity);
			complexIndex.add(entity, id);
		}

		return new IndexerImpl<E, T>(complexIndex, index);
	}

	private Index makeIndex() {
		return new TreeIndex();
	}

	@Override
	public void each(Visitor<E> visitor) {
		forEach(visitor);
	}

	@Override
	public void addTrigger(TriggerAction action, Trigger<E> trigger) {
		switch (action) {
		case BEFORE_INSERT:
			beforeInsert = U.expand(beforeInsert, trigger);
			break;

		case BEFORE_UPDATE:
			beforeUpdate = U.expand(beforeUpdate, trigger);
			break;

		case BEFORE_DELETE:
			beforeDelete = U.expand(beforeDelete, trigger);
			break;

		case BEFORE_READ:
			beforeRead = U.expand(beforeRead, trigger);
			break;

		case AFTER_INSERT:
			afterInsert = U.expand(afterInsert, trigger);
			break;

		case AFTER_UPDATE:
			afterUpdate = U.expand(afterUpdate, trigger);
			break;

		case AFTER_DELETE:
			afterDelete = U.expand(afterDelete, trigger);
			break;

		case AFTER_READ:
			afterRead = U.expand(afterRead, trigger);
			break;

		default:
			throw Errors.notExpected();
		}
	}

	@Override
	public PropertyInfo[] props() {
		return props;
	}

}
