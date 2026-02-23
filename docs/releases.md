# Releases

Flowman releases are done on an irregular basis, whenever new features are available or important issues have been
fixed.

## Official Releases

You will find all official Flowman releases on [GitHub](https://github.com/dimajix/flowman/releases).


## Version Policy

Starting with version 1.0.0, Flowman uses a semantic version schema. This means that Flowman uses a three-digit
version number *major*.*minor*.*bugfix*. The meaning of the components is as follows:

* The *major* number is increased in two scenarios: Either Flowman has implemented a big new feature set, or some
  changes lead to incompatibility with previous versions (breaking changes).
* The *minor* number is increased to indicating new features or slightly changed behavior.
* The *bugfix* number is increased for bug fixes. Otherwise, the behavior and feature set of Flowman remain unchanged.

Generally, you can expect Flowman to be downwards compatible with older releases within the same major version. Some
minor changes or bug fixes may change the behavior of some features, which have been changed intentionally (for example,
even a small bug fix intentionally changes the behavior).

Breaking changes will be documented in the changelog file for each version.


## Changelog (excerpt)

The following gives an (incomplete) list of past releases of the last 12 months. This will help you to spot breaking 
changes over time.

### Version 1.3.7

* Upgrade Spark to 3.5.6


### Version 1.3.6 - 2026-01-19

* Fix MySQL plugin configuration file
* Fix handling of Hive tables with missing directory
* Update several Maven plugin dependencies
* Update several library depdendencies


### Version 1.3.5 - 2025-08-28

* Add new `compareSchema` flag to `compare` target.


### Version 1.3.4 - 2025-08-06

* Fix column order in explode mapping
* Upgrade several dependencies


### Version 1.3.3 - 2025-05-12

* Upgrade SQL Server connector
* Upgrade Spark to 3.3.4
* Upgrade Spark to 3.4.4
* Upgrade Spark to 3.5.5


### Version 1.3.2 - 2025-02-07

* Fix documentation of SQL mapping
* Fix skipping of execution targets with "update" mode
* Use UTF-8 for pushing metrics to Prometheus


### Version 1.3.1 - 2024-12-19

* github-514: Generated SQL code for views should quote all column names


### Version 1.3.0 - 2024-12-06

* github-510: Upgrade Cloudera Spark 3.3 build profile


### Version 1.2.0 - 2024-04-03

* github-464: Upgrade Cloudera CDP 7.1 to Hotfix 16
* github-465: Fix parsing of Swagger schema files
* github-447: Support Spark 3.5.0
* github-444: Remove Flowman Hub
* github-443: Remove Flowman DSL
* github-466: Use frontend-maven-plugin for all npm packages
* github-467: Implement "target_records" metric for "copy" target
* github-474: Remove Flowman Studio
* github-480: Upgrade Spark to 3.5.1
* github-481: Fix Sphinx documentation


### Version 1.1.0 - 2023-10-10

* github-413: Support Azure Key Vault for retrieving secrets
* github-415: Improve documentation for Velocity templating
* github-361: Remove broken support for Databricks
* github-402: Support Spark 3.4
* github-417: Fix URL to flowman.io in all Maven modules
* github-416: Support specifying multiple targets separated by commas on CLI
* github-414: Support AWS Secrets Manager for retrieving secrets
* github-419: Add a command line option to kernel server to listen on specific address
* github-326: Truncate target graphs at sinks
* github-410: Support relative paths in project imports
* github-421: Provide new 'session' environment variable
* github-422: Upgrade Spark to 3.4.1
* github-423: Migrating a MariaDB/MySQL table from a text type to a numeric type fails
* github-425: Support building and running Flowman with Java 17
* github-388: Replace Akka http with Jersey/Jetty in Flowman History Server
* github-385: Update Flowman Tutorial
* github-412: Create tutorial for using Flowman Maven Plugin
* github-371: Automate EMR integration test via AWS CLI
* github-431: Update EMR build profil to 6.12
* github-428: Move project selector into top bar in Flowman History Server
* github-433: Add Trino JDBC driver as plugin
* github-434: Use sshj instead of ganymed for sftp
* github-452: [BUG] SQL assertions do not support empty strings as expected values
* github-450: Update Spark to 3.3.3
* github-454: Fix scope of netty-codec
* github-446: Fix deadlock when running targets in parallel

### Breaking changes

This version introduces some (minor) breaking changes:
* When providing sample records (for example, via the `values` mapping, or in the expected outcome of `sql` assertions),
  empty strings will be interpreted as empty strings. Older versions interpreted empty strings as SQL NULL values. In
  case you still need a NULL value, you can simply use the YAML `null` value.


### Version 1.0.1 - Upcoming

* github-411: [BUG] commons-text has wrong groupId in pom.xml


### Version 1.0.0 - 2023-04-27

* github-314: Move avro related functionality into separate plugin
* github-307: Describe Flowmans security policy in SECURITY.md
* github-315: Create build profile for CDP 7.1 with Spark 3.3
* github-317: Perform retry on failing JDBC commands
* github-318: Support mappings from different projects and with non-standard outputs in SQL
* github-140: Strictly check imports
* github-316: Beautify README.md
* github-310: Explain versioning policy in CHANGELOG.md
* github-313: Improve example for "observe" mapping
* github-319: Support Oracle for History Server
* github-320: Do not fall back to "inline" schema when no kind is specified
* github-321: Properly support lower case / upper case table names in Oracle
* github-309: Automate integration tests
* github-322: Remove flowman-client
* github-324: Log environment variables for imported projects
* github-329: Create Kernel API
* github-330: Implement Kernel Server
* github-331: Implement Kernel Client
* github-332: Build Flowman Shell on top of kernel Client/Server
* github-334: Create standalone Flowman Kernel application
* github-338: Update Spark to 3.3.2
* github-333: Forward Logs from Kernel to Client
* github-339: Set Copyright to "The Flowman Authors"
* github-345: [BUG] Loading an embedded schema inside a jar file should not throw an exception
* github-346: Create build profile for Databricks
* github-343: Log all client requests in kernel
* github-342: Automatically close session when client disconnects from kernel
* github-351: [BUG] Failing execution listener instantiation should not fail a build
* github-347: Exclude AWS SDK for Databricks and EMR build profiles
* github-352: [BUG] Spark sessions should not contain duplicate jars from different plugins
* github-353: Successful runs should not use System.exit(0)
* github-354: Optionally load custom log4j config from jar
* github-358: Provide different log4j config for Flowman server and kernel
* github-359: Update jline dependency
* github-357: Spark session should not be shut down in Databricks environment
* github-360: Logging should exclude more Databricks specific stuff
* github-361: Work around low-level API differences in DataBricks
* github-363: HiveDatabaseTarget should accept an optional location
* github-311: Create integration test for EMR
* github-362: Upgrade EMR to 6.10
* github-369: [BUG] Prevent endless loop in Kernel client, when getContext fails
* github-370: The Kernel client should use temporary workspaces with automatic cleanup
* github-337: Add documentation for flowman-rshell
* github-336: Add documentation for flowman-kernel
* github-366: Feature parity between Flowman shell and Flowman remote shell
* github-365: Implement saving mappings in Flowman Kernel/client
* github-367: Create integration test for "quickstart" archetype
* github-375: [BUG] "project reload" does not work correctly in remote shell with nested directories
* github-376: Document options to parallelize work
* github-378: Remove travis-ci integration
* github-308: Revise branching model
* github-381: Remove json-smart dependency
* github-382: [BUG] Parallel execution of multiple dq checks runs too many checks on Java 17
* github-384: Improve documentation for using docker-compose
* github-377: Load override config/env from .flowman-env.yml
* github-344: Support .flowman-ignore file for Flowman Kernel client
* github-385: Update Flowman tutorial
* github-386: Create Integration Test for Azure Synapse
* github-387: Remove scala-arm dependency
* github-390: Rename "master" branch to "main"
* github-392: [BUG] 'relation' mapping should support numeric partition values
* github-393: Move Maven archetype to flowman-maven project
* github-394: [BUG] The Spark job group and description are not set for sql assertions
* github-395: Support optional file locations for project imports
* github-397: Automate build using GitHub actions
* github-403: Upgrade Spark 3.2 to 3.2.4
* github-404: [BUG] Partition columns do not support Timestamp data type
* github-409: [BUG] Fix build for AWS EMR 6.10 and Azure Synapse 3.3
* github-407: Update Delta to 2.3.0 for Spark 3.3
* github-406: Improve integration tests to automatically pick up the current Flowman version
* github-408: Make use of DeltaLake in Synapse integration test
* github-405: Document deployment to EMR and Azure Synapse

#### Breaking changes

This version introduces some (minor) breaking changes:
* All Avro related functionality is now moved into the new "flowman-avro" plugin. If you rely on such functionality,
  you explicitly need to include the plugin in the `default-namesapce.yml` file.
* Imports are now strictly checked. This means when you cross-reference some entity in your project which is provided
  by a different Flowman project, you now need to explicitly import the project in the `project.yml`


### Version 0.30.2 - 2024-07-29

* github-496: updated build profile for CDP 7.1.9 platform version


### Version 0.30.1 - 2023-04-12

* github-379: [BUG] Parallel execution of multiple targets runs too many targets on Java 17
* github-383: Flowman should preserve target ordering


### Version 0.30.0 - 2023-01-03

* github-278: Parallelize execution of data quality checks. This also introduces a new configuration property
  `flowman.execution.check.parallelism` (default `1`)
* github-282: Improve implementation for counting records
* github-288: Support reading local CSV files from fatjar
* github-290: Simplify specifying project name in fatjar
* github-291: Simplify create/destroy Relation interface
* github-292: Upgrade AWS EMR to 6.9
* github-289: Color log output via log4j configuration (requires log4j 2.x)
* Bump postgresql from 42.4.1 to 42.4.3 in /flowman-plugins/postgresql
* Bump loader-utils from 1.4.0 to 1.4.2
* Bump json5 from 2.2.1 to 2.2.3
* github-293: [BUG] Fatal exceptions in parallel mapping instantiation cause deadlock
* github-273: Support projects contained in (fat) jar files
* github-294: [BUG] Parallel execution should not execute more targets after errors
* github-295: Create build profile for CDP 7.1 with Spark 3.2
* github-296: Update npm dependencies (vuetify & co)
* github-297: Parametrize when to execute a specific phase
* github-299: Move migrationPolicy and migrationStrategy from target into relation
* github-115: Implement additional build policy in relation target for forcing dirty. This also introduces a new
  configuration property `flowman.default.target.buildPolicy` (default `COMPAT`).
* github-298: Support fine-grained control when to execute each target of a job
* github-300: Implement new 'observe' mapping
* github-301: Upgrade Spark to 3.2.3
* github-302: Upgrade DeltaLake to 2.2.0
* github-303: Use multi-stage build for Docker image
* github-304: Upgrade Cloudera profile to CDP 7.1.8
* github-312: Fix build with Spark 2.4 and Maven 3.8

This version is fully backward compatible until and including version 0.27.0.


### Version 0.29.0 - 2022-11-08

* github-260 Remove hive-storage-api from several plugins and lib
* github-261: Add descriptions to all pom.xml
* github-262: Verification of "relation" targets should only check existence
* github-263: Add filter condition to data quality checks in documentation
* github-265: Make JDBC dialects pluggable
* github-264: Provide "jars" for all plugins
* github-267: Add new flowman-spark-dependencies module to simplify dependency management
* github-269: Create 'iterativeSql' mapping
* github-270: Upgrade Spark to 3.3.1
* github-271: Upgrade Delta to 2.1.1
* github-272: Create build profile for AWS EMR 6.8.0
* github-273: Refactor file abstraction
* github-274: Print Flowman configuration to console

This version is fully backward compatible until and including version 0.27.0.


### Version 0.28.0 - 2022-10-07

* Improve support for MariaDB / MySQL as data sinks
* github-245: Bump ejs, @vue/cli-plugin-babel, @vue/cli-plugin-eslint and @vue/cli-service in /flowman-studio-ui
* github-246: Bump ejs, @vue/cli-plugin-babel, @vue/cli-plugin-eslint and @vue/cli-service in /flowman-server-ui
* github-247: Automatically generate YAML schemas as part of build process
* github-248: Bump scss-tokenizer and node-sass in /flowman-server-u
* github-249: Add new options -X and -XX to increase logging
* github-251: Support for log4j2 Configuration
* github-252: Move sftp target into separate plugin
* github-253: SQL Server relation should support explicit staging table
* github-254: Use DATETIME2 for timestamps in MS SQL Server
* github-256: Provide Maven archetype for simple Flowman projects
* github-258: Support clustered indexes in MS SQL Server

This version is fully backward compatible until and including version 0.27.0.


### Version 0.27.0 - 2022-09-09

* github-232: [BUG] Column descriptions should be propagates in UNIONs
* github-233: [BUG] Missing Hadoop dependencies for S3, Delta, etc
* github-235: Implement new `rest` hook with fine control
* github-229: A build target should not fail if Impala "COMPUTE STATS" fails
* github-236: 'copy' target should not apply output schema
* github-237: jdbcQuery relation should use fields "sql" and "file" instead of "query"
* github-239: Allow optional SQL statement for creating jdbcTable
* github-238: Implement new 'jdbcCommand' target
* github-240: [BUG] Data quality checks in documentation should not fail on NULL values
* github-241: Throw an error on duplicate entity definitions
* github-220: Upgrade Delta-Lake to 2.0 / 2.1
* github-242: Switch to Spark 3.3 as default
* github-243: Use alternative Spark MS SQL Connector for Spark 3.3
* github-244: Generate project HTML documentation with optional external CSS file

This version breaks full backward compatibility with older versions! See more details below.

#### Breaking changes

In order to provide more flexibility and to increase consistency, the properties of `jdbcQuery` have changed. Before
this version, you needed to specify the SQL query in the `query` field. Now you have to specify the SQL query either
in the `sql` field or provide an external file and provide the file name in the `file` field. This new syntax is more
consistent with `jdbcView` and `hiveView` relations.


### Version 0.26.1 - 2022-08-03

* github-226: Upgrade to Spark 3.2.2
* github-227: [BUG] Flowman should not fail with field names containing "-", "/" etc
* github-228: Padding and truncation of CHAR(n)/VARCHAR(n) should be configurable

This version is fully backward compatible until and including version 0.26.0.


### Version 0.26.0 - 2022-07-27

* github-202: Add support for Spark 3.3
* github-203: [BUG] Resource dependencies for Hive should be case-insensitive
* github-204: [BUG] Detect indirect dependencies in a chain of Hive views
* github-207: [BUG] Build should not directly fail if inferring dirty status fails
* github-209: [BUG] HiveViews should not trigger cascaded refresh during CREATE phase even when nothing is changed
* github-211: Implement new `hiveQuery` relation
* github-210: [BUG] HiveTables should be migrated if partition columns change
* github-208: Implement JDBC hook for database based semaphores
* github-212: [BUG] Hive views should not be migrated in RELAXED mode if only comments have changed
* github-214: Update ImpalaJDBC driver to 2.6.26.1031
* github-144: Support changing primary key for JDBC relations
* github-216: [BUG] Floats should be represented as FLOAT and not REAL in MySQL/MariaDB
* github-217: Support collations for creating/migrating JDBC tables
* github-218: [BUG] Postgres dialect should be used for Postgres JDBC URLs
* github-219: [BUG] SchemaMapping should retain incoming comments
* github-215: Support COLUMN STORE INDEX for MS SQL Server
* github-182: Support column descriptions in JDBC relations (SQL Server / Azure SQL)
* github-224: Support column descriptions for MariaDB / MySQL databases
* github-223: Support column descriptions for Postgres database
* github-205: Initial support Oracle DB via JDBC
* github-225: [BUG] Staging schema should not have comments

This version breaks full backward compatibility with older versions! See more details below.

#### Breaking changes

We take backward compatibility very seriously. But sometimes a breaking change is needed to clean up code and to
enable new features. This release contains some breaking changes, which are annoying but simple to fix.
In order to respect `null` as keyword in YAML with a special semantics, some entities needed to be renamed, as
described in the following table:

| category | old kind | new kind |
|----------|----------|----------|
| mapping  | null     | empty    |
| relation | null     | empty    |
| target   | null     | empty    |
| store    | null     | none     |
| history  | null     | none     |


### Version 0.25.1 - 2022-06-15

* github-195: [BUG] Metric "target_records" is not reset correctly after an execution phase is finished
* github-197: [BUG] Impala REFRESH METADATA should not fail when dropping views

This version is fully backward compatible until and including version 0.24.0.


### Version 0.25.0 - 2022-05-31

* github-184: Only read in *.yml / *.yaml files in module loader
* github-183: Support storing SQL in external file in `hiveView`
* github-185: Missing _SUCCESS file when writing to dynamic partitions
* github-186: Support output mode `OVERWRITE_DYNAMIC` for Delta relation
* github-149: Support creating views in JDBC with new `jdbcView` relation
* github-190: Replace logo in documentation
* github-188: Log detailed timing information when writing to JDBC relation
* github-191: Add user provided description to quality checks
* github-192: Provide example queries for JDBC metric sink

This version is fully backward compatible until and including version 0.24.0.


### Version 0.24.1 - 2022-04-28

* github-175: '--jobs' parameter starts way to many parallel jobs
* github-176: start-/end-date in report should not be the same
* github-177: Implement generic SQL schema check
* github-179: Update DeltaLake dependency to 1.2.1

This version is fully backward compatible until and including version 0.24.0.


### Version 0.24.0 - 2022-04-05

* github-168: Support optional filters in data quality checks
* github-169: Support sub-queries in filter conditions
* github-171: Parallelize loading of project files
* github-172: Update CDP7 profile to the latest patch level
* github-153: Use non-privileged user in Docker image
* github-174: Provide application for generating YAML schema

This version breaks full backward compatibility with older versions! See more details below.

#### Breaking changes

We take backward compatibility very seriously. But sometimes a breaking change is needed to clean up code and to
enable new features. This release contains some breaking changes, which are annoying but simple to fix.
In order to avoid YAML schema inconsistencies, some entities needed to be renamed, as described in the following
table:

| category | old kind     | new kind             |
|----------|--------------|----------------------|
| mapping  | const        | values               |
| mapping  | empty        | null                 |
| mapping  | read         | relation             |
| mapping  | readRelation | relation             |
| mapping  | readStream   | stream               |
| relation | const        | values               |
| relation | empty        | null                 |
| relation | jdbc         | jdbcTable, jdbcQuery |
| relation | table        | hiveTable            |
| relation | view         | hiveView             |
| schema   | embedded     | inline               |


### Version 0.23.1 - 2022-03-28

* github-154: Fix failing migration when PK requires change due to data type
* github-156: Recreate indexes when data type of column changes
* github-155: Project level configs are used outside job
* github-157: Fix UPSERT operations for SQL Server
* github-158: Improve non-nullability of primary key column
* github-160: Use sensible defaults for default documenter
* github-161: Improve schema caching during execution
* github-162: ExpressionColumnCheck does not work when results contain NULL values
* github-163: Implement new column length quality check

This version is fully backward compatible until and including version 0.18.0.


### Version 0.23.0 - 2022-03-18

* github-148: Support staging table for all JDBC relations
* github-120: Use staging tables for UPSERT and MERGE operations in JDBC relations
* github-147: Add support for PostgreSQL
* github-151: Implement column level lineage in documentation
* github-121: Correctly apply documentation, before/after and other common attributes to templates
* github-152: Implement new 'cast' mapping

This version is fully backward compatible until and including version 0.18.0.


### Version 0.22.0 - 2022-03-01

* Add new `sqlserver` relation
* Implement new documentation subsystem
* Change default build to Spark 3.2.1 and Hadoop 3.3.1
* Add new `drop` target for removing tables
* Speed up project loading by reusing Jackson mapper
* Implement new `jdbc` metric sink
* Implement schema cache in Executor to speed up documentation and similar tasks
* Add new config variables `flowman.execution.mapping.schemaCache` and `flowman.execution.relation.schemaCache`
* Add new config variable `flowman.default.target.verifyPolicy` to ignore empty tables during VERIFY phase
* Implement initial support for indexes in JDBC relations

This version is fully backward compatible until and including version 0.18.0.


### Version 0.21.1 - 2022-01-28

* `flowexec` now returns different exit codes depending on the processing result

This version is fully backward compatible until and including version 0.18.0.


### Version 0.21.0 - 2022-01-26

* Fix wrong dependencies in Swagger plugin
* Implement basic schema inference for local CSV files
* Implement new `stack` mapping
* Improve error messages of local CSV parser

This version is fully backward compatible until and including version 0.18.0.


### Version 0.20.1 - 2022-01-06

* Implement detection of dependencies introduced by schema

This version is fully backward compatible until and including version 0.18.0.


### Version 0.20.0 - 2022-01-05

* Fix detection of Derby metastore to truncate comment lengths.
* Add new config variable `flowman.default.relation.input.columnMismatchPolicy` (default is `IGNORE`)
* Add new config variable `flowman.default.relation.input.typeMismatchPolicy` (default is `IGNORE`)
* Add new config variable `flowman.default.relation.output.columnMismatchPolicy` (default is `ADD_REMOVE_COLUMNS`)
* Add new config variable `flowman.default.relation.output.typeMismatchPolicy` (default is `CAST_ALWAYS`)
* Improve handling of `_SUCCESS` files for detecting (non-)dirty directories
* Implement new `merge` target
* Implement merge operation for Delta relations
* Implement merge operation for JDBC relations (only for some databases, i.e. MS SQL)
* Add new config variable `flowman.execution.target.useHistory` (default is `false`)
* Change the semantics of config variable `flowman.execution.target.forceDirty` (default is `false`)
* Add new `-d` / `--dirty` option for explicitly marking individual targets as dirty

This version is fully backward compatible until and including version 0.18.0.


### Version 0.19.0 - 2021-12-13

* Add build profile for Hadoop 3.3
* Add build profile for Spark 3.2
* Allow SQL expressions as dimensions in `aggregate` mapping
* Update Hive views when the resulting schema would change
* Add new `mapping cache` command to FlowShell
* Support embedded connection definitions
* Much improved Flowman History Server
* Fix wrong metric names with TemplateTarget
* Implement more `template` types for `connection`, `schema`, `dataset`, `assertion` and `measure`
* Implement new `measure` target for creating custom metrics for measuring data quality
* Add new config option `flowman.execution.mapping.parallelism`

This version is fully backward compatible until and including version 0.18.0.


### Version 0.18.0 - 2021-10-13

* Improve automatic schema migration for Hive and JDBC relations
* Improve support of `CHAR(n)` and `VARCHAR(n)` types. Those types will now be propagated to Hive with newer Spark versions
* Support writing to dynamic partitions for file relations, Hive tables, JDBC relations and Delta tables
* Fix the name of some config variables (floman.* => flowman.*)
* Added new config variables `flowman.default.relation.migrationPolicy` and `flowman.default.relation.migrationStrategy`
* Add plugin for supporting DeltaLake (https://delta.io), which provides `deltaTable` and `deltaFile` relation types
* Fix non-deterministic column order in `schema` mapping, `values` mapping and `values` relation
* Mark Hive dependencies has 'provided', which reduces the size of dist packages
* Significantly reduce size of AWS dependencies in AWS plugin
* Add new build profile for Cloudera CDP-7.1
* Improve Spark configuration of `LocalSparkSession` and `TestRunner`
* Update Spark 3.0 build profile to Spark 3.0.3
* Upgrade Impala JDBC driver from 2.6.17.1020 to 2.6.23.1028
* Upgrade MySQL JDBC driver from 8.0.20 to 8.0.25
* Upgrade MariaDB JDBC driver from 2.2.4 to 2.7.3
* Upgrade several Maven plugins to latest versions
* Add new config option `flowman.workaround.analyze_partition` to workaround CDP 7.1 issues
* Fix migrating Hive views to tables and vice-versa
* Add new option "-j <n>" to allow running multiple job instances in parallel
* Add new option "-j <n>" to allow running multiple tests in parallel
* Add new `uniqueKey` assertion
* Add new `schema` assertion
* Update Swagger libraries for `swagger` schema
* Implement new `openapi` plugin to support OpenAPI 3.0 schemas
* Add new `readHive` mapping
* Add new `simpleReport` and `report` hook
* Implement new templates


### Version 0.17.0 - 2021-06-02

* New Flowman Kernel and Flowman Studio application prototypes
* New ParallelExecutor
* Fix before/after dependencies in `count` target
* Default build is now Spark 3.1 + Hadoop 3.2
* Remove build profiles for Spark 2.3 and CDH 5.15
* Add MS SQL Server plugin containing JDBC driver
* Speed up file listing for `file` relations
* Use Spark JobGroups
* Better support running Flowman on Windows with appropriate batch scripts
