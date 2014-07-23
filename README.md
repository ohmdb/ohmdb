OhmDB - The Irresistible RDBMS + NoSQL Database for Java
=====

OhmDB is "the irresistible database" that offers the power of relational databases and the flexibility of NoSQL databases.

- OhmDB is very easy to use: Ohm.db("my.db").table(Item.class).insert(new Item("item1"));

- OhmDB supports ACID transactions!

- A single join has O(1) time complexity. A combination of multiple joins is internally processed as graph traversal with smart query optimization. 

- Just add ohmdb.jar to your project (release to Maven Central comming soon), and you are ready!

- All data is stored in a single file (e.g. my.db), which makes backups/duplications super-simple!

