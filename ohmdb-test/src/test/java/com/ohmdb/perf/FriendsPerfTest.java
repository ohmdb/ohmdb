package com.ohmdb.perf;

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

import org.testng.annotations.Test;

import com.ohmdb.abstracts.Any;
import com.ohmdb.abstracts.RWRelation;
import com.ohmdb.api.Db;
import com.ohmdb.api.Ohm;
import com.ohmdb.dsl.join.JoinBuilder;
import com.ohmdb.dsl.join.JoinConfig;
import com.ohmdb.dsl.join.JoinQuery;
import com.ohmdb.dsl.join.LinkMatcher;
import com.ohmdb.impl.OhmDBImpl;
import com.ohmdb.join.futureid.IDS;
import com.ohmdb.test.TestAny;
import com.ohmdb.test.TestCommons;
import com.ohmdb.util.Measure;

public class FriendsPerfTest extends TestCommons {

	private static final int USERS_N = 1 * 10000;

	private static final int FRIENDS_N = 25;

	@Test(timeOut = 60000)
	public void shouldMatchNthConnections() throws Exception {
		Db db2 = Ohm.db();
		LinkMatcher matcher = ((OhmDBImpl) db2).getLinkMatcher();

		RWRelation rel = relation(db2, "friends");
		rel.kind(true, true, true);

		System.out.println("db ready");

		Measure.start(USERS_N * FRIENDS_N);

		int retries = 0;
		for (int i = 0; i < USERS_N; i++) {
			for (int j = 0; j < FRIENDS_N; j++) {
				while (!rel.link(i, rnd(USERS_N))) {
					retries++;
				}
			}
		}

		Measure.finish("link");

		System.out.println("friends linked, retries: " + retries);

		final int total = 1000;

		Any<?> any1 = any(0, USERS_N);
		Any<?> any2 = any(0, USERS_N);
		Any<?> any3 = any(0, USERS_N);

		JoinQuery query = JoinBuilder.make().join(0, rel, 1, rel, 2, rel, 3).join(3, rel, 4).build();

		JoinConfig[] configs = new JoinConfig[total];

		for (int i = 0; i < total; i++) {
			int a = rnd(USERS_N);
			int b = rnd(USERS_N);

			configs[i] = jparam(query, IDS.futureIds(nums(a)), IDS.futureIds(any1), IDS.futureIds(any2),
					IDS.futureIds(any3), IDS.futureIds(nums(b)));
		}

		System.out.println("config ready!");

		Measure.start(total);

		for (int i = 0; i < total; i++) {
			matcher.exists(configs[i]);
		}

		Measure.finish("Nth connection");
	}

	private TestAny any(int from, int to) {
		return new TestAny(from, to);
	}

}
