package com.ohmdb.dsl;

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

import com.ohmdb.api.JoinResult;
import com.ohmdb.api.Ohm;
import com.ohmdb.api.Db;
import com.ohmdb.api.OneToMany;
import com.ohmdb.api.Search;
import com.ohmdb.api.Table;
import com.ohmdb.test.Tag;

class Item {
	String title;
	boolean published;

	public Item() {
	}

	public Item(String title) {
		this.title = title;
	}
}

public class ExamplesTest {

	Db db2 = Ohm.db("my.db");
	Table<Tag> tags = db2.table(Tag.class);
	Item item1 = null;
	Item item2 = null;
	Tag tag1 = null;
	Tag tag2 = null;

	private void eg1() {

		// Easy to use|

		Db db = Ohm.db("my.db");
		Table<Item> items = db.table(Item.class);
		long id = items.insert(new Item("item1"));

		/***/

		// Type-safe & readable queries|

		Item i = items.queryHelper();
		Search<Item> it = items.where(i.title).eq("item1").and(i.published).eq(true);

		/***/

		// Relations|

		OneToMany<Item, Tag> tagged = db.oneToMany(items, "tagged", tags);
		tagged.link(item1, tag1);
		tagged.delink(item2, tag2);

		/***/

		// Joins|

		Search<Item> publishedItems = items.where(i.published).eq(true);
		JoinResult publishedItemsWithTags = db.join(publishedItems, tagged, tags.all()).all();

		/***/

		// Your custom triggers in Java|

		db.before(Item.class).inserted().run(new LoggingTrigger<Item>());

		/***/

	}
}
