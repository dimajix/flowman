# Connectors

Flowman supports a broad range of data sources and sinks. Some are available directly in Flowman while others are
contained in [plugins](../plugins/index.md) to decrease code bloat when not required.

## Overview

The following table gives an overview of all currently supported data sources and sinks:

| Data Source    | Supports Read  | Supports Write | Plugin                                       | Relation                                                                                         |
|----------------|----------------|----------------|----------------------------------------------|--------------------------------------------------------------------------------------------------|
| AWS S3         | yes            | yes            | [AWS](../plugins/aws.html)                   | [`file`](../spec/relation/file.html)                                                             |
| Avro files     | yes            | yes            | N/A                                          | [`file`](../spec/relation/file.html)                                                             |
| Azure ABS      | yes            | yes            | [Azure](../plugins/azure.html)               | [`file`](../spec/relation/file.html)                                                             |
| Azure SQL      | yes            | yes            | [MS SQL Server](../plugins/mssqlserver.html) | [`sqlserver`](../spec/relation/sqlserver.html)                                                   |
| CSV files      | yes            | yes            | N/A                                          | [`file`](../spec/relation/file.html)                                                             |
| Delta Lake     | yes            | yes            | [Delta](../plugins/delta.html)               | [`deltaFile`](../spec/relation/deltaFile.html), [`deltaTable`](../spec/relation/deltaTable.html) |
| HDFS           | yes            | yes            | N/A                                          | [`file`](../spec/relation/file.html)                                                             |
| Hive           | yes            | yes            | N/A                                          | [`hiveTable`](../spec/relation/hiveTable.html), [`hiveView`](../spec/relation/hiveView.html)     |
| Impala         | yes (via Hive) | yes (via Hive) | [Impala](../plugins/impala.html)             | [`hiveTable`](../spec/relation/hiveTable.html), [`hiveView`](../spec/relation/hiveView.html)     |
| JSON files     | yes            | yes            | N/A                                          | [`file`](../spec/relation/file.html)                                                             |
| Kafka          | yes            | yes            | [Kafka](../plugins/kafka.html)               | [`kafka`](../spec/relation/kafka.html)                                                           |
| Local files    | yes            | yes            | N/A                                          | [`local`](../spec/relation/local.html)                                                           |
| MariaDB        | yes            | yes            | [MariaDB](../plugins/mariadb.html)           | [`jdbc`](../spec/relation/jdbcTable.html)                                                        |
| MySQL          | yes            | yes            | [MySQL](../plugins/mysql.html)               | [`jdbc`](../spec/relation/jdbcTable.html)                                                        |
| ORC files      | yes            | yes            | N/A                                          | [`file`](../spec/relation/file.html)                                                             |
| Parquet files  | yes            | yes            | N/A                                          | [`file`](../spec/relation/file.html)                                                             |
| PostgreSQL     | yes            | yes            | N/A                                          | [`jdbc`](../spec/relation/jdbcTable.html)                                                        |
| SQL Server     | yes            | yes            | [MS SQL Server](../plugins/mssqlserver.html) | [`sqlserver`](../spec/relation/sqlserver.html)                                                   |
| Sequence files | yes            | yes            | N/A                                          | [`file`](../spec/relation/file.html)                                                             |
| Text files     | yes            | yes            | N/A                                          | [`file`](../spec/relation/file.html)                                                             |
