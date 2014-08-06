OhmDB - The Irresistible RDBMS + NoSQL Database for Java
=====

OhmDB is "the irresistible database" that offers the power of relational databases and the flexibility of NoSQL databases.

# Apache Public License v2

OhmDB is released under the liberal APL v2 license, so it is free to use for both commercial and non-commercial projects.

# Using with Maven

Add the following snippet to the `<dependencies>` section in pom.xml:

```xml
<dependency>
    <groupId>com.ohmdb</groupId>
    <artifactId>ohmdb-all</artifactId>
    <version>1.0.0</version>
</dependency>
```

# Quick start

* Add the `ohmdb-all` dependency to your Maven project (as described above).

* Add the following code to your project, and execute it:
 
```java
import com.ohmdb.api.*;

class Person { public String name; public int age; }

public class Main {
	public static void main(String[] args) {
		Db db = Ohm.db("ohm.db");
		Table<Person> persons = db.table(Person.class);
		Person $p = persons.queryHelper();

		Person p1 = new Person();
		p1.name = "Niko";
		p1.age = 30;
		long id = persons.insert(p1);
		Person p2 = persons.get(id);

		persons.createIndexOn($p.age);
		Person[] adults = persons.where($p.age).gte(18).get();
	}
}
```

# Features

* Simple setup, no configuration (just add it as Maven dependency, and you are ready!)

* Delightful API:

```java
Ohm.db("ohm.db").table(Item.class).insert(new Item("item1"));
``` 

or just:

```java
DB.insert(new Item("item1"));
```

* ACID transactions with automatic recovery

* Fast joins through graph-based relations with graph traversal optimization 

* Automatic schema migration

* Single-file storage (e.g. my.db), so "backup" == "copy the file!"

# Contributing

1. Fork (and then `git clone https://github.com/ohmdb/ohmdb.git`).
2. Create a branch (`git checkout -b branch_name`).
3. Commit your changes (`git commit -am "Description of contribution"`).
4. Push to the branch (`git push origin branch_name`).
5. Open a Pull Request.
6. Thank you for your contribution! Wait for a response...
