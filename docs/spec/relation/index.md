---
layout: page
title: Flowman Relation Specifications
permalink: /spec/relation/index.html
---
# Flowman Relation Specification
Physical data sources (like Hive tables, relational SQL databases, files etc) are specified
using so called *relations*. Data flows can read and write from and to relations via 
appropriate mappings ([Read Mapping](../mapping/read.html)) or output operations ([Relation
Output](../output/relation.md)).

A relation always contains all required information to connect to the data source (for 
example the table name and a [JDBC Connection](../connection(jdbc.html)) in case of a JDBC
relation).

Some relations (like files, Hive tables and views) can also be directly created from Flowman,
thereby providing a single tool for schema management and data ingestion into tables. 


## Supported Relation Types

Flowman directly provides support for the most important data sources, which are also 
supported directly by Spark. Additional data sources can be implemented as plugins if
required.

* [`table`](table.html): 
A [Hive Table Relation](table.html) provides a Hive table for reading and/or writing
purpose.

* [`view`](view.html): 
A [Hive View Relation](view.html) provides a Hive view for reading purpose (Hive does not
support writing to views).

* [`file`](file.html):
A [HDFS File Relation](file.html) provides a file based relation backed by a HDFS or a 
compatible file system (like S3) using appropriate file formats (either text based like 
CSV or JSON or binary formats like Parquet, ORC, Avro). All file formats supported by Spark 
can be managed with a file relation. 

* [`jdbc`](jdbc.html): 
A [JDBC Relation](jdbc.html) is useful for integrating classical relation SQL databases like
MariaDB, Postgres, Microsoft SQL Server, Oracle etc.

* [`local`](local.html): 
A [Local File Relation](local.html) is a special relation referring to files on the local
filesystem where the Flowman CLI is run from. This can be useful for integrating data files
as part of a Flowman project specification, which are not available in a distributed
file system like HDFS or S3. 
