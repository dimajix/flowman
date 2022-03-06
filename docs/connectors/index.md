# Connectors

Flowman supports a broad range of data sources and sinks. Some are available directly in Flowman while others are
contained in [plugins](../plugins/index.md) to decrease code bloat when not required.

## Overview

The following table gives an overview of all currently supported data sources and sinks:

| Data Source    | Supports Read  | Supports Write | Plugin                                     | Relation                                                                                     |
|----------------|----------------|----------------|--------------------------------------------|----------------------------------------------------------------------------------------------|
| AWS S3         | yes            | yes            | [AWS](../plugins/aws.md)                   | [`file`](../spec/relation/file.md)                                                           |
| Avro files     | yes            | yes            | N/A                                        | [`file`](../spec/relation/file.md)                                                           |
| Azure ABS      | yes            | yes            | [Azure](../plugins/azure.md)               | [`file`](../spec/relation/file.md)                                                           |
| Azure SQL      | yes            | yes            | [MS SQL Server](../plugins/mssqlserver.md) | [`sqlserver`](../spec/relation/sqlserver.md)                                                 |
| CSV files      | yes            | yes            | N/A                                        | [`file`](../spec/relation/file.md)                                                           |
| Delta Lake     | yes            | yes            | [Delta](../plugins/delta.md)               | [`deltaFile`](../spec/relation/deltaFile.md), [`deltaTable`](../spec/relation/deltaTable.md) |
| HDFS           | yes            | yes            | N/A                                        | [`file`](../spec/relation/file.md)                                                           |
| Hive           | yes            | yes            | N/A                                        | [`hiveTable`](../spec/relation/hiveTable.md)                                                 |
| Impala         | yes (via Hive) | yes (via Hive) | [Impala](../plugins/impala.md)             | [`hiveTable`](../spec/relation/hiveTable.md)                                                 |
| JSON files     | yes            | yes            | N/A                                        | [`file`](../spec/relation/file.md)                                                           |
| Kafka          | yes            | yes            | [Kafka](../plugins/kafka.md)               | [`kafka`](../spec/relation/kafka.md)                                                         |
| Local files    | yes            | yes            | N/A                                        | [`local`](../spec/relation/local.md)                                                         |
| MariaDB        | yes            | yes            | [MariaDB](../plugins/mariadb.md)           | [`jdbc`](../spec/relation/jdbcTable.md)                                                      |
| MySQL          | yes            | yes            | [MySQL](../plugins/mysql.md)               | [`jdbc`](../spec/relation/jdbcTable.md)                                                      |
| ORC files      | yes            | yes            | N/A                                        | [`file`](../spec/relation/file.md)                                                           |
| Parquet files  | yes            | yes            | N/A                                        | [`file`](../spec/relation/file.md)                                                           |
| PostgreSQL     | yes            | yes            | N/A                                        | [`jdbc`](../spec/relation/jdbcTable.md)                                                      |
| SQL Server     | yes            | yes            | [MS SQL Server](../plugins/mssqlserver.md) | [`sqlserver`](../spec/relation/sqlserver.md)                                                 |
| Sequence files | yes            | yes            | N/A                                        | [`file`](../spec/relation/file.md)                                                           |
| Text files     | yes            | yes            | N/A                                        | [`file`](../spec/relation/file.md)                                                           |
