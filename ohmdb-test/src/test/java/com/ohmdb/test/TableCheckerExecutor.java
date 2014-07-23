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

import java.util.Date;

import com.ohmdb.api.Table;
import com.ohmdb.statistical.RelationChecker;
import com.ohmdb.statistical.TableChecker;

@SuppressWarnings("rawtypes")
public class TableCheckerExecutor implements Parallel {

	private final DatabaseCheck databaseCheck;

	public TableCheckerExecutor(DatabaseCheck databaseCheck) {
		this.databaseCheck = databaseCheck;
	}

	private Object shadow;

	private Object checker;

	@SuppressWarnings("unchecked")
	@Override
	public void init(int threadN) {
		checker = databaseCheck.checkers.get(threadN - 1);
		if (checker instanceof TableChecker) {
			shadow = new TableShadow<Object>(((TableChecker<Object>) checker).getClazz(), databaseCheck.insider);
		} else {
			shadow = new RelationShadow(((RelationChecker) checker).getRelationName(), databaseCheck.insider);
		}
		System.out.println("Initialized checker: " + checker);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run(int threadN, int cycleN) {
		databaseCheck.lock.readLock().lock();

		if (cycleN % 1000 == 0) {
			System.out.println(cycleN + " @ " + new Date() + " : " + checker);
		}

		// System.out.println("# " + cycleN + " IN " + checker.name);

		if (checker instanceof TableChecker) {
			TableChecker<?> tableChecker = (TableChecker<?>) checker;
			((TableShadow<?>) shadow).setTable((Table) databaseCheck.db().table(tableChecker.clazz));
			tableChecker.check((TableShadow) shadow, cycleN + 1);
		} else {
			RelationChecker relationChecker = (RelationChecker) checker;
			((RelationShadow) shadow).setRelation(TestCommons.relation(databaseCheck.db(),
					relationChecker.getRelationName()));
			relationChecker.check((RelationShadow) shadow, cycleN + 1);
		}

		// System.out.println("# " + cycleN + " OUT " + checker.name);

		databaseCheck.lock.readLock().unlock();
	}

	public Object getShadow() {
		return shadow;
	}

}
