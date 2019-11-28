# Version 0.11.0
* Add support for Spark 3.0 preview
* Remove HBase plugin
* Add optional 'filter' to 'readRelation' mapping
* Improve Hive compatibility of SQL generator
* Fix target table search in Hive Union Table

# Version 0.10.2
* Improve Impala catalog support

# Version 0.10.1
* Add 'error' output to 'extractJson' mapping

# Version 0.10.0
* Add new 'hiveUnionTable' relation
* Add new 'union' schema
* Support nested columns in deduplication
* Support nested Hive VIEWs
* Support Spark 2.4.4

# Version 0.9.1
* Fix wrong Spark application name if configured via Spark config

# Version 0.9.0
* Complete overhaul of job execution. No tasks any more
* Improve Swagger schema support

# Version 0.8.3
* Add configuration option for column insert position of 'historize' mapping

# Version 0.8.2
* Add optional filter condition to 'latest' mapping

# Version 0.8.1
* Improve generation of SQL code containing window functions

# Version 0.8.0
* Add new metric system
* Add Hive view generation from mappings
* Support for Hadoop 3.1 and 3.2 (without Hive)
* Add 'historize' mapping

# Version 0.7.1
* Fix build with CDH-5.15 profile

# Version 0.7.0
* Implement initial REST server
* Implement initial prototype of UI
* Implement new datasets for new tasks (copy/compare/...)

# Version 0.6.5
* Add support for checkpoint directory

# Verison 0.6.4
* Implement column renaming in projections

# Verison 0.6.3
* CopyRelationTask also performs projection

# Version 0.6.2
* "explode" mapping supports simple data types

# Version 0.6.1
* Fix NPE in ShowRelationTask

# Version 0.6.0

* Add multiple relations to `showRelation` task
* github-33: Add new `unit` mapping
* github-34: Fix NPE in Swagger schema reader
* github-36: Add new `explode` mapping

# Version 0.5.0 - 2019-05-28

* github-32: Improve handling of nullable structs
* github-30: Refactoring of whole specification handling
* github-30: Add new 'template' mapping
* Add new `flatten` entry in assembler
* Implement new `flatten` mapping
* github-31: Fix handling of local project definition in flowexec


# Version 0.4.1 - 2019-05-11

* Add parameters to "job run" CLI
* Fix error handling of failing "build" and "clean" tasks


# Version 0.4.0 - 2019-05-08

* Add support for Spark 2.4.2
* Add new assemble mapping
* Add new conform mapping


# Version 0.3.0 - 2019-03-20

* Add null format for Spark
* Add deployment


# Version 0.1.2 - 2018-11-05

* Update to Spark 2.3.2
* Small fixes


# Version 0.1.1 - 2018-09-14

Small fixes


# Version 0.1.0 - 2018-07-20

Initial release
