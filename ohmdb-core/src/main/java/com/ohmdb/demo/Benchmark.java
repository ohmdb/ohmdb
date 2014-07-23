package com.ohmdb.demo;

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

import java.io.File;
import java.util.Random;

import com.ohmdb.api.Ids;
import com.ohmdb.api.Join;
import com.ohmdb.api.ManyToMany;
import com.ohmdb.api.Ohm;
import com.ohmdb.api.OhmDB;
import com.ohmdb.api.Table;

public class Benchmark {

	private static final Random RND = new Random();

	private static String DB_FILENAME;

	public static void main(String[] args) {
		int usersN = args.length > 0 ? Integer.parseInt(args[0]) : 1000;
		int friendsN = args.length > 1 ? Integer.parseInt(args[1]) : 50;
		DB_FILENAME = args.length > 2 ? args[2] : "ohmdb-demo.db";

		System.out.println(usersN + " users, each has " + friendsN + " friends in average\n");

		new File(DB_FILENAME).delete();

		insert(usersN);
		update(usersN);
		set(usersN);

		long[] all = makeFriends(usersN, friendsN / 2);

		OhmDB db = Ohm.db(DB_FILENAME);

		joinNth(db, usersN, all, 1);
		joinNth(db, usersN, all, 2);
		joinNth(db, usersN, all, 3);
		joinNth(db, usersN, all, 4);

		db.shutdown();
	}

	private static void insert(int usersN) {
		OhmDB db = Ohm.db(DB_FILENAME);

		Table<DemoUser> users = db.table(DemoUser.class);

		Measure.start(usersN);

		DemoUser user = new DemoUser();

		for (int i = 0; i < usersN; i++) {
			user.setUsername("user" + i);
			user.setAge(i % 100);
			users.insert(user);
		}

		db.shutdown();

		Measure.finish("insert");
		System.out.println();
	}

	private static void update(int usersN) {
		OhmDB db = Ohm.db(DB_FILENAME);

		Table<DemoUser> users = db.table(DemoUser.class);

		Measure.start(usersN);

		DemoUser user = new DemoUser();

		for (int i = 0; i < usersN; i++) {
			user.setUsername("theuser" + i);
			user.setAge(i % 1000);
			users.update(i, user);
		}

		db.shutdown();

		Measure.finish("update");
		System.out.println();
	}

	private static void set(int usersN) {
		OhmDB db = Ohm.db(DB_FILENAME);

		Table<DemoUser> users = db.table(DemoUser.class);

		Measure.start(usersN);

		for (int i = 0; i < usersN; i++) {
			users.set(i, "age", 123);
		}

		db.shutdown();

		Measure.finish("set");
		System.out.println();
	}

	private static void joinNth(OhmDB db, int usersN, long[] all, int n) {
		ManyToMany<Object, Object> fr = db.manyToManySymmetric(null, "friends", null);

		Ids<Object> x = db.all(all);
		Ids<Object> y = db.all(all);
		Ids<Object> z = db.all(all);

		int total = 100;

		Measure.start(total);

		int connN = 0;

		for (int i = 0; i < total; i++) {
			long a = RND.nextInt(usersN);
			long b = RND.nextInt(usersN);

			Join join;
			switch (n) {
			case 1:
				join = db.join(db.ids(a), fr, db.ids(b));
				break;

			case 2:
				join = db.join(db.ids(a), fr, x).join(x, fr, db.ids(b));
				break;

			case 3:
				join = db.join(db.ids(a), fr, x).join(x, fr, y).join(y, fr, db.ids(b));
				break;

			case 4:
				join = db.join(db.ids(a), fr, x).join(x, fr, y).join(y, fr, z).join(z, fr, db.ids(b));
				break;

			default:
				throw new IllegalArgumentException("Wrong N!");
			}

			boolean connected = join.exists();

			if (connected) {
				connN++;
			}
		}

		System.out.println(connN + " of " + total + " random couples were connected at rank " + n);

		Measure.finish(n + " join(s)");
		System.out.println();
	}

	private static long[] makeFriends(int usersN, int friendsN) {
		OhmDB db = Ohm.db(DB_FILENAME);
		ManyToMany<Object, Object> fr = db.manyToManySymmetric(null, "friends", null);

		Measure.start(usersN * friendsN);

		long[] all = new long[usersN];
		for (int i = 0; i < usersN; i++) {
			all[i] = i;
			for (int j = 0; j < friendsN; j++) {
				fr.link(i, RND.nextInt(usersN));
			}
		}

		db.shutdown();

		Measure.finish("link");
		System.out.println();
		return all;
	}

}
