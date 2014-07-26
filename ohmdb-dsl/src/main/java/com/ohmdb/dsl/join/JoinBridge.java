package com.ohmdb.dsl.join;

import com.ohmdb.abstracts.FutureIds;
import com.ohmdb.abstracts.Numbers;

public interface JoinBridge {

	Numbers reach(long[] combo, int level, FutureIds futureIds);

}
