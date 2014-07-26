package com.ohmdb.test;

/*
 * #%L
 * ohmdb-test
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

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.ohmdb.api.Ohm;
import com.ohmdb.api.Db;
import com.ohmdb.api.Table;
import com.ohmdb.statistical.RelationChecker;
import com.ohmdb.statistical.TableChecker;
import com.ohmdb.util.U;

public class DatabaseCheck extends TestCommons {

	private static final String FILENAME = "/tmp/check.db";

	private Db db;

	private final int reloadInterval;

	List<Object> checkers = new ArrayList<Object>();

	final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

	private volatile boolean stop = false;

	private final int threadsFactor;

	TestInsider insider = new TestInsider();

	public DatabaseCheck(int reloadInterval, int threadsFactor) {
		this.reloadInterval = reloadInterval;
		this.threadsFactor = threadsFactor;
		new File(FILENAME).delete();
		db = Ohm.db(FILENAME);
	}

	public void reload() {
		lock.writeLock().lock();
		synchronized (this) {
			System.out.println("########################### Reloading " + FILENAME);
			db.shutdown();
			U.sleep(1000);
			db = Ohm.db(FILENAME);
			System.out.println("########################### Reloaded " + FILENAME);
		}
		lock.writeLock().unlock();
	}

	public <E> void register(TableChecker<E> tableChecker) {
		checkers.add(tableChecker);
	}

	public void register(RelationChecker relationChecker) {
		checkers.add(relationChecker);
	}

	public void clear() {
		checkers.clear();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void execute(final int total) {

		Thread refresher = new Thread(new Runnable() {
			@Override
			public void run() {
				if (reloadInterval > 0) {
					System.out.println("Started refresher");

					while (!stop) {

						try {
							Thread.sleep(reloadInterval);
						} catch (Exception e) {
							e.printStackTrace();
							return;
						}

						if (!stop) {
							reload();
						}
					}
				}
			}
		});

		Object[] args = { this };
		ThreadPack[] packs = new ThreadPack[threadsFactor];

		for (int i = 0; i < packs.length; i++) {
			packs[i] = threads("read", checkers.size(), total, true, TableCheckerExecutor.class, args);
		}

		for (int i = 0; i < packs.length; i++) {
			packs[i].start();
		}

		refresher.start();

		ThreadPack.finish(packs);

		stop = true;

		try {
			refresher.join();
		} catch (InterruptedException e) {
			System.out.println("INTERUPTED REFRESHER!");
		}

		U.sleep(1000);
		System.out.println("############# VALIDATING TABLES AND RELATIONS... #############");

		Set<Object> verified = new HashSet();

		lock.writeLock().lock();

		for (Parallel parallel : packs[0].getParallels()) {
			TableCheckerExecutor executor = (TableCheckerExecutor) parallel;
			Object shadow = executor.getShadow();

			if (shadow instanceof TableShadow) {
				TableShadow<?> tableShadow = (TableShadow) shadow;
				Class<?> clazz = tableShadow.getClazz();

				if (!verified.contains(clazz)) {
					tableShadow.setTable((Table) db().table(clazz));
					tableShadow.validate();
					verified.add(clazz);
				}
			} else {
				RelationShadow relShadow = (RelationShadow) shadow;

				String name = relShadow.getRelationName();

				if (!verified.contains(name)) {
					relShadow.setRelation(relation(db(), name));
					relShadow.validate();
					verified.add(name);
				}

			}
		}

		lock.writeLock().unlock();
		System.out.println("############# VALIDATION DONE! #############");
	}

	public synchronized Db db() {
		return db;
	}

	public TestInsider getInsider() {
		return insider;
	}
	
}
