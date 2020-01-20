# Relations

Physical data sources (like Hive tables, relational SQL databases, files etc) are specified
using so called *relations*. Data flows can read and write from and to relations via 
appropriate mappings ([Read Mapping](../mapping/read-relation.md)) or build targets ([Relation
Target](../target/relation.md)).

A relation always contains all required information to connect to the data source (for 
example the table name and a [JDBC Connection](../connection/jdbc.md) in case of a JDBC
relation).

Some relations (like files, Hive tables and views) can also be directly created from Flowman,
thereby providing a single tool for schema management and data ingestion into tables. 


## Relation Types

Flowman directly provides support for the most important data sources, which are also 
supported directly by Spark. Additional data sources can be implemented as plugins if
required.

```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:

   *
```
