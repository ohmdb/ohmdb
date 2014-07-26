package com.ohmdb.dsl;

import com.ohmdb.abstracts.RWRelation;
import com.ohmdb.api.Ids;
import com.ohmdb.api.Join;
import com.ohmdb.api.JoinMode;
import com.ohmdb.api.ManyToMany;
import com.ohmdb.api.ManyToOne;
import com.ohmdb.api.OneToMany;
import com.ohmdb.api.OneToOne;
import com.ohmdb.api.Parameter;
import com.ohmdb.api.Relation;
import com.ohmdb.api.Table;
import com.ohmdb.dsl.impl.ParamImpl;
import com.ohmdb.dsl.join.JoinImpl;
import com.ohmdb.dsl.join.LinkMatcher;
import com.ohmdb.dsl.rel.ManyToManyImpl;
import com.ohmdb.dsl.rel.ManyToOneImpl;
import com.ohmdb.dsl.rel.OneToManyImpl;
import com.ohmdb.dsl.rel.OneToOneImpl;

public abstract class OhmDbDSL {

	protected final LinkMatcher linkMatcher;

	public OhmDbDSL(LinkMatcher linkMatcher) {
		this.linkMatcher = linkMatcher;
	}

	public <FROM, TO> ManyToOne<FROM, TO> manyToOne(Table<FROM> from, String name, Table<TO> to) {
		return new ManyToOneImpl<FROM, TO>(relation(from, name, to, false, true, false));
	}

	public <FROM, TO> OneToMany<FROM, TO> oneToMany(Table<FROM> from, String name, Table<TO> to) {
		return new OneToManyImpl<FROM, TO>(relation(from, name, to, false, false, true));
	}

	public <FROM, TO> ManyToMany<FROM, TO> manyToMany(Table<FROM> from, String name, Table<TO> to) {
		return new ManyToManyImpl<FROM, TO>(relation(from, name, to, false, true, true));
	}

	public <FROM_TO> ManyToMany<FROM_TO, FROM_TO> manyToManySymmetric(Table<FROM_TO> from, String name,
			Table<FROM_TO> to) {
		return new ManyToManyImpl<FROM_TO, FROM_TO>(relation(from, name, to, true, true, true));
	}

	public <FROM, TO> OneToOne<FROM, TO> oneToOne(Table<FROM> from, String name, Table<TO> to) {
		return new OneToOneImpl<FROM, TO>(relation(from, name, to, false, false, false));
	}

	public <FROM_TO> OneToOne<FROM_TO, FROM_TO> oneToOneSymmetric(Table<FROM_TO> from, String name, Table<FROM_TO> to) {
		return new OneToOneImpl<FROM_TO, FROM_TO>(relation(from, name, to, true, false, false));
	}

	public <T> Parameter<T> param(String name, Class<T> type) {
		return new ParamImpl<T>(name, type);
	}

	public <FROM, TO> Join join(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to) {
		return new JoinImpl(linkMatcher, from, relation, to, JoinMode.INNER);
	}

	public <FROM, TO> Join leftJoin(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to) {
		return new JoinImpl(linkMatcher, from, relation, to, JoinMode.LEFT_OUTER);
	}

	public <FROM, TO> Join rightJoin(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to) {
		return new JoinImpl(linkMatcher, from, relation, to, JoinMode.RIGHT_OUTER);
	}

	public <FROM, TO> Join fullJoin(Ids<FROM> from, Relation<FROM, TO> relation, Ids<TO> to) {
		return new JoinImpl(linkMatcher, from, relation, to, JoinMode.FULL_OUTER);
	}

	protected abstract <FROM, TO> RWRelation relation(Table<FROM> from, String name, Table<TO> to, boolean symmetric,
			boolean manyFroms, boolean manyTos);

}
