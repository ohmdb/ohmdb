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

import com.ohmdb.abstracts.ReadOnlyRelation;
import com.ohmdb.join.JoinBuilder;
import com.ohmdb.join.JoinConfig;
import com.ohmdb.join.JoinQuery;
import com.ohmdb.test.TestCommons;
import com.ohmdb.util.Measure;

public class LinkMatcherPerfTest extends TestCommons {

	@Test
	public void shouldMatchFriends() {
		ReadOnlyRelation rel1 = rel1(db);
		ReadOnlyRelation rel2 = rel2(db);

		JoinQuery query = JoinBuilder.make().join(0, rel1, 1, rel2, 2).build();

		int count = 3000;
		Measure.start(count);

		for (int i = 0; i < count; i++) {
			JoinConfig params = jparam(query, nums(1, 2, 7), nums(30, 40, 90), nums(200, 333));
			matcher().match(params);
		}

		Measure.finish("match");
	}

}
