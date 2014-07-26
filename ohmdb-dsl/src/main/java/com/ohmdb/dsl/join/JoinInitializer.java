package com.ohmdb.dsl.join;

import com.ohmdb.abstracts.FutureIds;

public interface JoinInitializer {

	JoinSide[] optimize(AJoin[] joins, FutureIds[] futureIds);

}
