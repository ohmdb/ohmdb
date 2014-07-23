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
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Calendar;
import java.util.List;
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

	public static void finish(String name) {
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

		String data = String.format(" - %s '%s' took %s ms (avg. %s/sec)", count, name, ms, avgs);
		debug(data);
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

	private static void debug(String data) {
		Runtime rt = Runtime.getRuntime();
		long totalMem = rt.totalMemory();
		long maxMem = rt.maxMemory();
		long freeMem = rt.freeMemory();
		long usedMem = totalMem - freeMem;
		int megs = 1024 * 1024;

		String gcinfo = "";
		List<GarbageCollectorMXBean> gcs = ManagementFactory.getGarbageCollectorMXBeans();
		for (GarbageCollectorMXBean gc : gcs) {
			gcinfo += " | " + gc.getName() + " x " + gc.getCollectionCount() + " (" + gc.getCollectionTime() + "ms)";
		}

		String msg = "%s | MEM [total=%sMB, used=%sMB, max=%sMB] %s";
		String info = String.format(msg, data, totalMem / megs, usedMem / megs, maxMem / megs, gcinfo);

		System.out.println(info);
	}

}
