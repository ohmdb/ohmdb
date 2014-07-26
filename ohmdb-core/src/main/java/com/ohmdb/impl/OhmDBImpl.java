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

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.ohmdb.abstracts.DataLoader;
import com.ohmdb.abstracts.IdAddress;
import com.ohmdb.abstracts.IdColl;
import com.ohmdb.abstracts.LoadedData;
import com.ohmdb.abstracts.LockManager;
import com.ohmdb.abstracts.RWRelation;
import com.ohmdb.abstracts.RelationInternals;
import com.ohmdb.abstracts.TableInternals;
import com.ohmdb.api.Ids;
import com.ohmdb.api.Join;
import com.ohmdb.api.JoinMode;
import com.ohmdb.api.ManyToMany;
import com.ohmdb.api.ManyToOne;
import com.ohmdb.api.OhmDB;
import com.ohmdb.api.OneToMany;
import com.ohmdb.api.OneToOne;
import com.ohmdb.api.Op;
import com.ohmdb.api.Parameter;
import com.ohmdb.api.Relation;
import com.ohmdb.api.SearchCriteria;
import com.ohmdb.api.Table;
import com.ohmdb.api.Transaction;
import com.ohmdb.api.Trigger;
import com.ohmdb.api.TriggerAction;
import com.ohmdb.api.TriggerCreator;
import com.ohmdb.bean.PropertyInfo;
import com.ohmdb.codec.ByteArrCodec;
import com.ohmdb.codec.StoreCodec;
import com.ohmdb.dsl.impl.ParamImpl;
import com.ohmdb.dsl.rel.impl.ManyToManyImpl;
import com.ohmdb.dsl.rel.impl.ManyToOneImpl;
import com.ohmdb.dsl.rel.impl.OneToManyImpl;
import com.ohmdb.dsl.rel.impl.OneToOneImpl;
import com.ohmdb.exception.InvalidIdException;
import com.ohmdb.filestore.DataSource;
import com.ohmdb.filestore.DataStore;
import com.ohmdb.filestore.DatastoreTransaction;
import com.ohmdb.filestore.FileStore;
import com.ohmdb.filestore.FilestoreTransaction;
import com.ohmdb.filestore.NoDataStore;
import com.ohmdb.join.DefaultLinkMatcher;
import com.ohmdb.join.LinkMatcher;
import com.ohmdb.relation.RelationImpl;
import com.ohmdb.transaction.TransactionImpl;
import com.ohmdb.transaction.TransactorImpl;
import com.ohmdb.util.Errors;
import com.ohmdb.util.U;
import com.ohmdb.util.UTILS;

public class OhmDBImpl implements OhmDB, DataSource {

	private final StoreCodec<byte[]> VALUE_CODEC = new ByteArrCodec();

	private final OhmDBStats stats = new OhmDBStats();

	private final IdColl ids = new MapBackedIdColl(stats);

	private final Map<Class<?>, Table<?>> tables = new HashMap<Class<?>, Table<?>>();

	private final Map<String, RWRelation> relations = new HashMap<String, RWRelation>();

	private final List<Triggering<?>> triggers = new ArrayList<Triggering<?>>();

	private final DataStore store;

	private final LockManager lockManager = new LockManagerImpl(stats);

	private final TransactorImpl transactor = new TransactorImpl(this, lockManager, stats);

	private final LinkMatcher linkMatcher = new DefaultLinkMatcher();

	private final WeakReference<OhmDB> dbRef;

	private boolean isDown = false;

	// FIXME: increase limit
	private final ByteBuffer SER_HELPER = ByteBuffer.allocateDirect(1 * 1024 * 1024);

	private Throwable error;

	public OhmDBImpl(String filename) {
		dbRef = new WeakReference<OhmDB>(this);

		DataLoader loader = new DataLoader();
		this.store = new FileStore(filename, loader, VALUE_CODEC, stats, false, dbRef);

		LoadedData data = loader.getData();

		data.fillData(this);

		addShutdownHook();
	}

	public OhmDBImpl() {
		dbRef = new WeakReference<OhmDB>(this);

		this.store = new NoDataStore(this);

		addShutdownHook();
	}

	@Override
	public Transaction startTransaction() {
		FilestoreTransaction tx = store.transaction();
		TransactionImpl transaction = new TransactionImpl(transactor, tx);
		transaction.begin();
		return transaction;
	}

	@Override
	@SuppressWarnings("unchecked")
	public synchronized <T> Table<T> table(Class<T> clazz) {
		Table<T> table = (Table<T>) tables.get(clazz);

		if (table == null) {
			table = new TableImpl<T>(this, clazz, store, ids, transactor, stats, lockManager);
			tables.put(clazz, table);

			for (Triggering<?> triggering : triggers) {
				registerTriggers(triggering, table);
			}
		}

		return table;
	}

	@SuppressWarnings("unchecked")
	@Override
	public synchronized <T> Table<T> table(String fullClassname) {
		try {
			Class<T> clazz = (Class<T>) Class.forName(fullClassname);
			return table(clazz);
		} catch (ClassNotFoundException e) {
			throw Errors.rte(e);
		}
	}

	public synchronized void rollback() {
		for (Table<?> table : tables.values()) {
			TableInternals<?> internal = (TableInternals<?>) table;
			internal.rollback();
		}

		for (RWRelation rel : relations.values()) {
			RelationInternals internal = (RelationInternals) rel;
			internal.rollback();
		}
	}

	public synchronized void commit() {
		for (Table<?> table : tables.values()) {
			TableInternals<?> internal = (TableInternals<?>) table;
			internal.commit();
		}

		for (RWRelation rel : relations.values()) {
			RelationInternals internal = (RelationInternals) rel;
			internal.commit();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public synchronized RWRelation relation(Table<?> from, String name, Table<?> to) {
		// relation might be already created in LoadedData.fillData
		RWRelation rel = relations.get(name);

		if (rel == null) {
			rel = new RelationImpl(from, name, to, store, ids, transactor, lockManager, stats);
			relations.put(name, rel);
		}

		return rel;
	}

	private synchronized <FROM, TO> RWRelation relation(Table<FROM> from, String name, Table<TO> to, boolean symmetric,
			boolean manyFroms, boolean manyTos) {
		// relation might be already created in LoadedData.fillData
		RWRelation rel = relations.get(name);

		if (rel == null) {
			rel = new RelationImpl<FROM, TO>(from, name, to, store, ids, transactor, lockManager, stats, symmetric,
					manyFroms, manyTos);
			relations.put(name, rel);
		} else {
			rel.kind(symmetric, manyFroms, manyTos);
		}

		return rel;
	}

	@Override
	public <FROM, TO> ManyToOne<FROM, TO> manyToOne(Table<FROM> from, String name, Table<TO> to) {
		return new ManyToOneImpl<FROM, TO>(relation(from, name, to, false, true, false));
	}

	@Override
	public <FROM, TO> OneToMany<FROM, TO> oneToMany(Table<FROM> from, String name, Table<TO> to) {
		return new OneToManyImpl<FROM, TO>(relation(from, name, to, false, false, true));
	}

	@Override
	public <FROM, TO> ManyToMany<FROM, TO> manyToMany(Table<FROM> from, String name, Table<TO> to) {
		return new ManyToManyImpl<FROM, TO>(relation(from, name, to, false, true, true));
	}

	@Override
	public <FROM_TO> ManyToMany<FROM_TO, FROM_TO> manyToManySymmetric(Table<FROM_TO> from, String name,
			Table<FROM_TO> to) {
		return new ManyToManyImpl<FROM_TO, FROM_TO>(relation(from, name, to, true, true, true));
	}

	@Override
	public <FROM, TO> OneToOne<FROM, TO> oneToOne(Table<FROM> from, String name, Table<TO> to) {
		return new OneToOneImpl<FROM, TO>(relation(from, name, to, false, false, false));
	}

	@Override
	public <FROM_TO> OneToOne<FROM_TO, FROM_TO> oneToOneSymmetric(Table<FROM_TO> from, String name, Table<FROM_TO> to) {
		return new OneToOneImpl<FROM_TO, FROM_TO>(relation(from, name, to, true, false, false));
	}

	@Override
	public <FROM, TO> Join join(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to) {
		return new JoinImpl(linkMatcher, from, relation, to, JoinMode.INNER);
	}

	@Override
	public <FROM, TO> Join leftJoin(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to) {
		return new JoinImpl(linkMatcher, from, relation, to, JoinMode.LEFT_OUTER);
	}

	@Override
	public <FROM, TO> Join rightJoin(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to) {
		return new JoinImpl(linkMatcher, from, relation, to, JoinMode.RIGHT_OUTER);
	}

	@Override
	public <FROM, TO> Join fullJoin(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to) {
		return new JoinImpl(linkMatcher, from, relation, to, JoinMode.FULL_OUTER);
	}

	@Override
	public void delete(long id) {
		IdAddress address = address(id);
		address.table.delete(id);
	}

	private IdAddress address(long id) {
		IdAddress address = ids.get(id);

		if (address == null) {
			throw new InvalidIdException(id);
		}

		return address;
	}

	@Override
	public void update(Object entity) {
		long id = UTILS.getId(entity);
		IdAddress address = address(id);
		((TableInternals<?>) address.table).updateObj(entity);
	}

	@Override
	public <T> TriggerCreator<T> before(Class<T> type) {
		return new TriggerCreatorImpl<T>(this, type, true);
	}

	@Override
	public <T> TriggerCreator<T> after(Class<T> type) {
		return new TriggerCreatorImpl<T>(this, type, false);
	}

	@Override
	public <T> void trigger(Class<T> type, TriggerAction action, Trigger<T> trigger) {
		Triggering<T> triggering = new Triggering<T>(type, action, trigger);
		triggers.add(triggering);

		for (Table<?> table : tables.values()) {
			registerTriggers(triggering, table);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private <T> void registerTriggers(Triggering<T> triggering, Table<?> table) {
		TableInternals<?> tbl = (TableInternals<?>) table;
		if (triggering.type.isAssignableFrom(tbl.getClazz())) {
			tbl.addTrigger(triggering.action, (Trigger) triggering.trigger);
		}
	}

	public RWRelation relation(String name) {
		return relation(null, name, null);
	}

	@Override
	public <T> Parameter<T> param(String name, Class<T> type) {
		return new ParamImpl<T>(name, type);
	}

	@Override
	public SearchCriteria crit(String columnName, Op op, Object value) {
		return SearchCriteriaImpl.single(columnName, op, value);
	}

	@Override
	public <T> Ids<T> ids(long... ids) {
		return new IdsImpl<T>(ids);
	}

	@Override
	public <T> Ids<T> all(long... ids) {
		return new AnyIds<T>(ids);
	}

	public synchronized void stop() {
		store.stop();
	}

	@Override
	public synchronized void shutdown() {
		if (!isDown) {
			System.out.println("Shutting down database...");

			store.shutdown();
			isDown = true;

			System.out.println("Database stopped.");
		}

		if (error != null) {
			throw Errors.rte("Persistence error detected!", error);
		}
	}

	public LinkMatcher getLinkMatcher() {
		assert linkMatcher != null;
		return linkMatcher;
	}

	private void addShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new ShutdownThread(dbRef));
	}

	public synchronized byte[] exportRecord(long id) {
		SER_HELPER.clear();

		exportColumns(id);
		exportRelations(id);

		SER_HELPER.flip();
		byte[] bytes = new byte[SER_HELPER.limit()];
		SER_HELPER.get(bytes);

		// System.out.println("EXPORTING BYTES " + bytes.length);
		return bytes;
	}

	private void exportColumns(long id) {
		IdAddress address = ids.get(id);
		boolean exportTableData = address != null;

		U.encode(exportTableData, SER_HELPER);

		if (exportTableData) {
			TableInternals<?> table = (TableInternals<?>) address.table;
			PropertyInfo[] props = table.props();

			U.encode(table.getClazz().getCanonicalName(), SER_HELPER);
			U.encode(props.length, SER_HELPER);

			for (PropertyInfo prop : props) {
				String colName = prop.getName();
				Object colVal = prop.getColumn().get(address.row);
				U.encode(colName, SER_HELPER);
				U.encode(colVal, SER_HELPER);
			}
		}
	}

	private void exportRelations(long id) {
		U.encode(relations.size(), SER_HELPER);

		for (Entry<String, RWRelation> rel : relations.entrySet()) {
			U.encode(rel.getKey(), SER_HELPER);
			long[] links = rel.getValue().linksFrom(id).toArray();
			U.encode(links.length, SER_HELPER);
			for (long link : links) {
				U.encode(link, SER_HELPER);
			}
		}
	}

	public synchronized void importRecord(long id, byte[] bytes) {
		SER_HELPER.clear();

		SER_HELPER.put(bytes);
		SER_HELPER.flip();

		boolean importTableData = (Boolean) decode();

		if (importTableData) {
			importColumns(id);
		}

		importRelations(id);
	}

	private void importColumns(long id) {
		String fullName = (String) decode();
		int colN = (Integer) decode();

		Table<Object> table = table(fullName);
		TableInternals<?> table2 = (TableInternals<?>) table;

		for (int i = 0; i < colN; i++) {
			String colName = (String) decode();
			Object colVal = decode();
			table2.fill(id, colName, colVal);
		}
	}

	private void importRelations(long id) {
		int relN = (Integer) decode();

		for (int i = 0; i < relN; i++) {
			String relName = (String) decode();
			RelationInternals rel = (RelationInternals) relation(relName);
			int linksN = (Integer) decode();
			for (int j = 0; j < linksN; j++) {
				long linkTo = (Long) decode();
				rel.fill(id, linkTo);
			}
		}
	}

	private Object decode() {
		Object val = U.decode(SER_HELPER);
		// System.out.println("DECODE " + val);
		return val;
	}

	@Override
	public Object read(long id) {
		return exportRecord(id);
	}

	public synchronized void deleteRelsInTx(long id, DatastoreTransaction tx) {
		for (RWRelation rel : relations.values()) {
			RelationInternals rel2 = (RelationInternals) rel;
			rel2.deleteFromInTx(id, tx);
			rel2.deleteToInTx(id, tx);
		}
	}

	public synchronized void failure(Throwable e) {
		this.error = e;
	}

	@SuppressWarnings("unchecked")
	@Override
	public long insert(Object entity) {
		Table<Object> tbl = (Table<Object>) table(entity.getClass());
		return tbl.insert(entity);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T get(long id) {
		return (T) address(id).table.get(id);
	}

}
