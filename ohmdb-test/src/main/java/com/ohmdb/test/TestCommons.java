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
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

import com.google.common.base.Objects;
import com.ohmdb.abstracts.DatastoreTransaction;
import com.ohmdb.abstracts.FutureIds;
import com.ohmdb.abstracts.Numbers;
import com.ohmdb.abstracts.RWRelation;
import com.ohmdb.abstracts.ReadOnlyRelation;
import com.ohmdb.api.Db;
import com.ohmdb.api.Links;
import com.ohmdb.api.ManyToMany;
import com.ohmdb.api.ManyToOne;
import com.ohmdb.api.Ohm;
import com.ohmdb.api.OhmDB;
import com.ohmdb.api.OneToMany;
import com.ohmdb.api.Table;
import com.ohmdb.api.Transaction;
import com.ohmdb.api.TransactionListener;
import com.ohmdb.dsl.join.DefaultJoinConfig;
import com.ohmdb.dsl.join.JoinConfig;
import com.ohmdb.dsl.join.JoinQuery;
import com.ohmdb.dsl.join.LinkMatcher;
import com.ohmdb.impl.OhmDBImpl;
import com.ohmdb.join.futureid.IDS;
import com.ohmdb.numbers.Nums;
import com.ohmdb.util.Check;
import com.ohmdb.util.Errors;
import com.ohmdb.util.U;
import com.ohmdb.util.UTILS;

public abstract class TestCommons {

	protected static final int X = 1000001;
	protected static final int Y = X + 1;
	protected static final int Z = X + 2;
	protected static final int W = X + 3;
	protected static final int Q = X + 4;

	protected static final String DB_FILE = tmpFile("ohm", ".db").getAbsolutePath();

	public static Table<?> TBL1 = new MockTable("TBL1", Nums.arrFromTo(0, 10));

	public static Table<?> TBL2 = new MockTable("TBL2", Nums.arrFromTo(0, 10));

	public static Table<?> TBL3 = new MockTable("TBL3", Nums.arrFromTo(0, 10));

	protected WeakReference<Db> DB_REF;

	protected static Numbers ids0to10;

	// make non-null for debugging sync
	private static final Object THREADS_SYNC = null; // TODO must be null for
														// real testing!

	protected static final Random RND = new Random();

	private static boolean verbose = true;

	protected Transaction TX;

	protected Db db;
	protected Table<Person> persons;
	protected Table<Book> books;
	protected Table<Tag> tags;

	protected ManyToMany<Person, Person> friends;

	protected OneToMany<Person, Book> wrote;
	protected ManyToOne<Book, Person> writtenBy;

	protected ManyToMany<Person, Tag> follows;
	protected ManyToMany<Tag, Person> followedBy;

	protected ManyToOne<Tag, Book> describes;
	protected OneToMany<Book, Tag> describedBy;

	protected Person $p;
	protected Book $b;
	protected Tag $t;

	protected <T> List<T> list(Iterator<T> it) {
		List<T> list = new ArrayList<T>();

		while (it.hasNext()) {
			list.add(it.next());
		}

		return list;
	}

	protected <T> void isNull(Object value) {
		Assert.assertNull(value);
	}

	protected <T> void isntNull(Object value) {
		Assert.assertNotNull(value);
	}

	protected <T> void isTrue(boolean cond) {
		Assert.assertTrue(cond);
	}

	protected <T> void isFalse(boolean cond) {
		Assert.assertFalse(cond);
	}

	protected static <T> void eq(long value, long expected) {
		eq(new Long(value), new Long(expected));
	}

	protected static <T> void eq(T value, T expected) {
		if (!Objects.equal(value, expected)) {
			Assert.assertEquals(value, expected);
		}
	}

	protected static <T> void eqs(long[] values, long... expected) {
		if (verbose) {
			System.out.println("EXPECTED: " + U.text(expected));
			System.out.println(" - FOUND: " + U.text(values));
		}
		Assert.assertEquals(values.length, expected.length);

		for (int i = 0; i < expected.length; i++) {
			Assert.assertEquals(values[i], expected[i]);
		}
	}

	protected static <T> void eqs(Object[] values, Object... expected) {
		if (verbose) {
			System.out.println("EXPECTED: " + U.text(expected));
			System.out.println(" - FOUND: " + U.text(values));
		}
		Assert.assertEquals(values.length, expected.length);

		for (int i = 0; i < expected.length; i++) {
			Assert.assertEquals(values[i], expected[i]);
		}
	}

	protected static <T> void eqv(T value, T expected) {
		if (verbose) {
			System.out.println("EXPECTED: " + expected);
			System.out.println(" - FOUND: " + value);
		}
		Assert.assertEquals(value, expected);
	}

	protected <T> void eq(List<T> list, T... values) {
		if (verbose) {
			System.out.println("EXPECTED: " + Arrays.toString(values));
			System.out.println(" - FOUND: " + list);
		}
		Assert.assertEquals(list.toArray(), values);
	}

	protected void eqnums(long[] numbers, long... values) {
		eq(numbers, values);
	}

	protected void eqnums(Numbers numbers, long... values) {
		detailedEqNums(numbers, values);

		detailedEqNums(Nums.unionAll(numbers), values);
		detailedEqNums(Nums.union(numbers, numbers), values);
		detailedEqNums(Nums.union(numbers, Nums.none()), values);
		detailedEqNums(Nums.unionAll(numbers, Nums.none(), numbers), values);
		detailedEqNums(Nums.intersectAll(new Numbers[] { numbers }), values);
		detailedEqNums(Nums.intersect(numbers, numbers), values);
	}

	private void detailedEqNums(Numbers numbers, long... values) {
		long[] arr = numbers.toArray();
		if (verbose) {
			System.out.println("EXPECTED: " + Arrays.toString(values));
			System.out.println(" - FOUND: " + Arrays.toString(arr));
		}
		Assert.assertEquals(arr, values);
	}

	protected void eqlinks(Links paths, long[]... fromTos) {
		eqlinks(paths, links(fromTos));
	}

	protected void eqnull(Object... values) {
		for (Object value : values) {
			Assert.assertNull(value);
		}
	}

	protected void eqlinks(Links paths, Links expected) {
		if (verbose) {
			System.out.println("EXPECTED: " + UTILS.toString(expected));
			System.out.println(" - FOUND: " + UTILS.toString(paths));
		}
		Assert.assertTrue(UTILS.equal(paths, expected));
	}

	protected void linksEQ(Links[] links, Links[] expected) {
		if (verbose) {
			System.out.println("EXPECTED: " + U.text(expected));
			System.out.println(" - FOUND: " + U.text(links));
		}
		isTrue(UTILS.equal(links, expected));
	}

	protected void neq(Links paths, Links notExpected) {
		if (verbose) {
			System.out.println("NOT EXPECTED: " + UTILS.toString(notExpected));
			System.out.println("     - FOUND: " + UTILS.toString(paths));
		}
		Assert.assertFalse(UTILS.equal(paths, notExpected));
	}

	public static void check(boolean state) {
		Assert.assertTrue(state);
	}

	public static void time(String msg) {
		System.out.println("=== " + msg + " " + new Date());
	}

	public static Iterator<Long> randomIterator(final int max, final int count) {
		return new RandomIterator(count, max);
	}

	public Numbers nums(long... numbers) {
		return Nums.from(numbers);
	}

	public static void print(Object obj) {
		System.out.println(U.text(obj));
	}

	public static void printn(Object... obj) {
		System.out.println(Arrays.toString(obj));
	}

	public static long[] ln(long... fromTo) {
		return fromTo;
	}

	public Links links(long[]... fromTos) {
		return UTILS.linksFromTos(fromTos);
	}

	public JoinConfig jparam(JoinQuery query, Numbers... ids) {
		return new DefaultJoinConfig(IDS.futureIds(ids), query.joins());
	}

	public JoinConfig jparam(JoinQuery query, FutureIds... futureIds) {
		return new DefaultJoinConfig(futureIds, query.joins());
	}

	public JoinConfig jparam0to10(JoinQuery query, Numbers... ids) {
		for (int i = 0; i < ids.length; i++) {
			if (ids[i] == null) {
				ids[i] = Nums.fromTo(0, 10);
			}
		}

		return jparam(query, ids);
	}

	public static Links ln(Links[] links) {
		Check.state(links.length == 1);
		return links[0];
	}

	public static Links[] nlinks(Links... links) {
		return links;
	}

	protected <T> Table<T> table(Class<T> clazz) {
		return db.table(clazz);
	}

	protected Table<Person> personsTable() {
		return db.table(Person.class);
	}

	protected Table<Book> booksTable() {
		return db.table(Book.class);
	}

	protected Table<Tag> tagsTable() {
		return db.table(Tag.class);
	}

	protected static Person person(String name, int age) {
		return new Person(name, age);
	}

	protected static Person2 person2(String name, int age) {
		return new Person2(name, age);
	}

	protected static Book book(String title, boolean published) {
		return new Book(title, published);
	}

	protected static Tag tag(String name) {
		return new Tag(name);
	}

	@DataProvider(name = "orderProvider")
	public Object[][] orderProvider() {
		int[] order1 = { 0, 1 };
		int[] order2 = { 1, 0 };

		Object[][] data = { { order1 }, { order2 } };
		return data;
	}

	@DataProvider(name = "orderProvider2")
	public Object[][] orderProvider2() {
		int[] order1 = { 0, 1, 2 };
		int[] order2 = { 0, 2, 1 };
		int[] order3 = { 1, 0, 2 };
		int[] order4 = { 1, 2, 0 };
		int[] order5 = { 2, 1, 0 };
		int[] order6 = { 2, 0, 1 };

		Object[][] data = { { order1 }, { order2 }, { order3 }, { order4 }, { order5 }, { order6 } };
		return data;
	}

	public static void setVerbose(boolean verbose) {
		TestCommons.verbose = verbose;
	}

	protected ThreadPack threads(String name, final int threadCount, final int cyclesCount, final boolean debug,
			final Parallel parallel) {

		final ThreadPack pack = new ThreadPack(name, threadCount, cyclesCount);

		for (int i = 0; i < threadCount; i++) {
			final int n = i + 1;
			pack.threads[i] = new Thread() {
				public void run() {
					if (debug) {
						System.out.println(" - started thread " + n + " / " + threadCount);
					}

					try {
						parallel.init(n);
						for (int j = 0; j < cyclesCount; j++) {
							if (THREADS_SYNC != null) {
								synchronized (THREADS_SYNC) {
									System.out.println("THREADS SYNC IN");
									parallel.run(n, j);
									System.out.println("THREADS SYNC OUT");
								}
							} else {
								parallel.run(n, j);
							}
						}
					} catch (Throwable e) {
						pack.error(e);
					}

					if (debug) {
						System.out.println(" - finished thread " + n + " / " + threadCount);
					}

					pack.done(parallel);
				}
			};
		}

		return pack;
	}

	protected ThreadPack threads(String name, final int threadCount, final int cyclesCount, final boolean debug,
			final Class<? extends Parallel> parallelClass, final Object[] args) {

		final ThreadPack pack = new ThreadPack(name, threadCount, cyclesCount);

		for (int i = 0; i < threadCount; i++) {
			final int n = i + 1;
			pack.threads[i] = new Thread() {
				public void run() {
					if (debug) {
						System.out.println(" - started thread " + n + " / " + threadCount);
					}

					Parallel parallel;
					try {
						parallel = (Parallel) parallelClass.getConstructors()[0].newInstance(args);
					} catch (Exception e) {
						throw Errors.rte(e);
					}

					try {
						parallel.init(n);
						for (int j = 0; j < cyclesCount; j++) {
							if (THREADS_SYNC != null) {
								synchronized (THREADS_SYNC) {
									System.out.println("THREADS SYNC IN");
									parallel.run(n, j);
									System.out.println("THREADS SYNC OUT");
								}
							} else {
								parallel.run(n, j);
							}
						}
					} catch (Throwable e) {
						pack.error(e);
					}

					if (debug) {
						System.out.println(" - finished thread " + n + " / " + threadCount);
					}

					pack.done(parallel);
				}
			};
		}

		return pack;
	}

	protected void reload() {
		db.shutdown();
		db = Ohm.db(DB_FILE);
		DB_REF = new WeakReference<Db>(db);
	}

	@BeforeMethod
	public void cleanDB() {
		System.out.println("vvvvvvvvvvvv STARTING TEST " + getClass() + " - DB CLEAN-UP vvvvvvvvvvvv");

		U.delete(DB_FILE);

		db = Ohm.db(DB_FILE);
		DB_REF = new WeakReference<Db>(db);

		ids0to10 = Nums.fromTo(0, 10);

		OhmDB.setDefaultDb(db);
		ready();
	}

	@AfterMethod
	public void stopDB() {
		System.out.println("--------------- STOPPING DB --------------\n");
		db.shutdown();
		System.out.println("^^^^^^^^^^^^^^^ STOPPED DB ^^^^^^^^^^^^^^^\n");
	}

	protected void ready() {
	}

	protected void initSchema() {
		persons = db.table(Person.class);
		books = db.table(Book.class);
		tags = db.table(Tag.class);

		friends = db.manyToMany(persons, "friends", persons);

		wrote = db.oneToMany(persons, "wrote", books);
		writtenBy = wrote.inversed();

		follows = db.manyToMany(persons, "follows", tags);
		followedBy = follows.inversed();

		describes = db.manyToOne(tags, "describes", books);
		describedBy = describes.inversed();

		$p = persons.queryHelper();
		$b = books.queryHelper();
		$t = tags.queryHelper();
	}

	protected void initSchemaInMem() {
		System.out.println("Creating in-mem DB");
		db = Ohm.db();
		initSchema();
	}

	protected void initIndexing() {
		persons.createIndexOn($p.age);
		persons.createIndexOn($p.name);
		tags.createIndexOn($t.name);
		books.createIndexOn($b.published);
	}

	protected void initData10() {
		fillData10();
		initIndexing();
	}

	protected void fillData10() {
		initSchema();

		Fixtures.nickN(persons, 10, true);
		Fixtures.bookN(books, 10, true);
		Fixtures.tagN(tags, 10, true);

		Fixtures.relN(wrote, 0, 10, 10);
		Fixtures.relN(describedBy, 10, 20, 10);
		Fixtures.relN(follows, 0, 20, 10);
	}

	protected void checkData10() {
		Fixtures.nickN(persons, 10, false);
		Fixtures.bookN(books, 10, false);
		Fixtures.tagN(tags, 10, false);

		// FIXME check rels
	}

	protected char rndChar() {
		return (char) (65 + rnd(26));
	}

	protected String rndStr(int length) {
		return rndStr(length, length);
	}

	protected String rndStr(int minLength, int maxLength) {
		int len = minLength + rnd(maxLength - minLength + 1);
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < len; i++) {
			sb.append(rndChar());
		}

		return sb.toString();
	}

	protected int rnd(int n) {
		return RND.nextInt(n);
	}

	protected int rndExcept(int n, int except) {
		Check.arg(n > 1 || except != 0, "Cannot produce such number!");
		while (true) {
			int num = RND.nextInt(n);
			if (num != except) {
				return num;
			}
		}
	}

	protected <T> T rnd(T[] arr) {
		return arr[rnd(arr.length)];
	}

	protected int rnd() {
		return RND.nextInt();
	}

	protected long rndL() {
		return RND.nextLong();
	}

	protected void expectedException() {
		Assert.fail("Expected exception!");
	}

	protected boolean yesNo() {
		return RND.nextBoolean();
	}

	protected void printIfNot(boolean expected, String errMsg) {
		if (!expected) {
			System.out.println(errMsg);
		}
	}

	protected void failIfNot(boolean expected, String errMsg) {
		if (!expected) {
			Assert.fail(errMsg);
		}
	}

	protected void tx() {
		TX = db.startTransaction();
	}

	protected void tx(AtomicInteger n) {
		TX = db.startTransaction();
		TX.addListener(txInc(n));
	}

	protected void rollback() {
		TX.rollback();
	}

	protected void commit() {
		TX.commit();
	}

	public String _(String format, Object... args) {
		return String.format(format, args);
	}

	protected void linky(Links[] links, Links[] links2) {
		System.out.println("=== REZ === " + Arrays.toString(links));
		System.out.println("=== EXP === " + Arrays.toString(links2));

		isTrue(UTILS.equal(links, links2));
	}

	protected void checkJoin(JoinConfig config, Links[] links2) {
		Links[] links = matcher().match(config);
		linky(links, links2);
	}

	protected long[] noln(int n) {
		return ln(n);
	}

	protected static RWRelation relation(Db db, Table<?> from, String name, Table<?> to) {
		return ((OhmDBImpl) db).relation(from, name, to);
	}

	public static RWRelation relation(Db db, String name) {
		return ((OhmDBImpl) db).relation(name);
	}

	public ReadOnlyRelation rel1(Db db) {
		RWRelation rel = relation(db, TBL1, "rel1", TBL2);

		UTILS.link(rel, 1, nums(20, 30));
		UTILS.link(rel, 2, nums(30, 40, 50));
		UTILS.link(rel, 7, nums(88));

		return rel;
	}

	public ReadOnlyRelation rel2(Db db) {
		RWRelation rel = relation(db, TBL2, "rel2", TBL3);

		UTILS.link(rel, 20, nums(200, 2000));
		UTILS.link(rel, 30, nums(333));
		UTILS.link(rel, 40, nums(400, 4000));
		UTILS.link(rel, 70, nums(777));

		return rel;
	}

	public ReadOnlyRelation rel3(Db db) {
		RWRelation rel = relation(db, TBL1, "rel3", TBL3);

		UTILS.link(rel, 1, nums(100, 2000));
		UTILS.link(rel, 2, nums(200, 2001));
		UTILS.link(rel, 3, nums(300, 3001));
		UTILS.link(rel, 4, nums(400, 4001));

		return rel;
	}

	public RWRelation rel10x10(String name, int fromBase, int toBase, Db db, Table<?> tbl1, Table<?> tbl2) {
		RWRelation rel = relation(db, tbl1, name, tbl2);

		for (int i = 0; i < 10; i++) {
			for (int j = 0; j <= 10; j++) {
				UTILS.link(rel, i + fromBase, nums(j + toBase));
			}
		}

		return rel;
	}

	public ReadOnlyRelation[] getR1R2(Db db) {
		return new ReadOnlyRelation[] { rel1(db), rel2(db) };
	}

	public ReadOnlyRelation[] getR1R2R3(Db db) {
		return new ReadOnlyRelation[] { rel1(db), rel2(db), rel3(db) };
	}

	public ReadOnlyRelation randomRel(Db db, String name, Table<?> tbl1, Table<?> tbl2, int max) {
		RWRelation rel = relation(db, tbl1, name, tbl2);

		rel.clear();
		int count = Nums.rnd(1, max);
		for (int i = 0; i < count; i++) {
			UTILS.link(rel, Nums.rnd(10), Nums.random(1, max, 0, max * 2));
		}

		return rel;
	}

	public ReadOnlyRelation[] randomRels(Db db, int max) {
		ReadOnlyRelation[] rels = new ReadOnlyRelation[Nums.rnd(2, 3)];

		Table<?> tbl = new MockTable("TBL", Nums.arrFromTo(0, 1000));

		for (int i = 0; i < rels.length; i++) {
			rels[i] = randomRel(db, "rel" + i, tbl, tbl, max);
		}

		return rels;
	}

	public Numbers ids0() {
		return nums(0, 9);
	}

	public Numbers ids1() {
		return nums(10, 11, 12, 13, 14, 15, 20, 30, 40, 50);
	}

	public Numbers ids2() {
		return nums(100, 200, 300, 333, 400, 777);
	}

	protected Person p(long id) {
		Person person = new Person();
		person.id = id;
		return person;
	}

	protected Book b(long id) {
		Book book = new Book();
		book.id = id;
		return book;
	}

	protected void haveIds(Object[] objs, int... ids) {
		eq(objs.length, ids.length);
		for (int i = 0; i < ids.length; i++) {
			hasId(objs[i], ids[i]);
		}
	}

	protected void hasId(Object obj, int id) {
		eq(U.getId(obj), id);
	}

	protected void waitTx(DatastoreTransaction... txs) {
		U.sleep(1000); // FIXME
	}

	protected TransactionListener txInc(AtomicInteger n) {
		return new IncTransactionListener(n);
	}

	protected AtomicInteger atomN() {
		return new AtomicInteger();
	}

	protected LinkMatcher matcher() {
		return ((OhmDBImpl) db).getLinkMatcher();
	}

	protected Map<Long, Object> storeMap() {
		return new HashMap<Long, Object>();
	}

	private static Object[][] numN(int n) {
		Object[][] data = new Object[n][];

		for (int i = 0; i < data.length; i++) {
			data[i] = new Object[] { i };
		}

		return data;
	}

	@DataProvider
	public static Object[][] num2() {
		return numN(2);
	}

	@DataProvider
	public static Object[][] num3() {
		return numN(3);
	}

	@DataProvider
	public static Object[][] num4() {
		return numN(4);
	}

	@DataProvider
	public static Object[][] num5() {
		return numN(5);
	}

	@DataProvider
	public static Object[][] num10() {
		return numN(10);
	}

	@DataProvider
	public static Object[][] num50() {
		return numN(50);
	}

	@DataProvider
	public static Object[][] num100() {
		return numN(100);
	}

	@DataProvider
	public static Object[][] num200() {
		return numN(200);
	}

	@DataProvider
	public static Object[][] num500() {
		return numN(500);
	}

	@DataProvider
	public static Object[][] num1000() {
		return numN(1000);
	}

	@DataProvider
	public static Object[][] num5000() {
		return numN(5000);
	}

	@DataProvider
	public static Object[][] num10000() {
		return numN(10000);
	}

	@DataProvider
	public static Object[][] num50000() {
		return numN(50000);
	}

	@DataProvider
	public static Object[][] num100000() {
		return numN(100000);
	}

	protected static File tmpFile(String prefix, String suffix) {
		try {
			File file = File.createTempFile(prefix, suffix);
			file.deleteOnExit();
			return file;
		} catch (IOException e) {
			throw Errors.rte(e);
		}
	}

}
