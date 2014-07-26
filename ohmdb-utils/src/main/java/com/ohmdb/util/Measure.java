package com.ohmdb.util;

/*
 * #%L
 * ohmdb-utils
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

import java.util.Calendar;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class Measure {

	public static class Stat {
		public double runs;
		public double time;
		public double best = -1;
		public double worst = Integer.MAX_VALUE;

		public double avg() {
			return runs / time;
		}
	}

	private static long start;
	private static int count;

	private static Map<String, Stat> stats = new TreeMap<String, Stat>();

	public static void start(int count) {
		Measure.count = count;
		start = Calendar.getInstance().getTimeInMillis();
	}

	public static long finish(String name) {
		long end = Calendar.getInstance().getTimeInMillis();
		long ms = end - start;

		if (ms == 0) {
			ms = 1;
		}

		double avg = ((double) count / (double) ms);

		String avgs = avg > 1 ? Math.round(avg) + "K" : Math.round(avg * 1000) + "";

		Stat stat = stat(name);
		stat.runs += count;
		stat.time += ms;
		stat.best = Math.max(stat.best, avg);
		stat.worst = Math.min(stat.worst, avg);

		long mem = Runtime.getRuntime().totalMemory() / 1024 / 1024;

		System.out.println(String.format(" - total time for %s '%s' operations: %s ms, average: %s/sec, mem=%sMB",
				count, name, ms, avgs, mem));

		return ms;
	}

	public static void finish() {
		finish("no-name");
	}

	public static Stat stat(String name) {
		Stat stat = stats.get(name);
		if (stat == null) {
			stat = new Stat();
			stats.put(name, stat);
		}
		return stat;
	}

	public static void stats() {
		for (Entry<String, Stat> entry : stats.entrySet()) {
			Stat stat = entry.getValue();
			System.out.println(String.format("FOR '%s': AVERAGE = %.0fK/sec, BEST = %.0fK/sec, WORST = %.0fK/sec",
					entry.getKey(), stat.avg(), stat.best, stat.worst));
		}
	}

	public static int getCount() {
		return count;
	}

}
