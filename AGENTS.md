# AGENTS.md

Guidance for AI coding agents working in the Flowman repository.

## What is Flowman?

Flowman is a declarative *data build tool* based on Apache Spark. Users describe data sources, sinks, transformations
(mappings), physical data models (relations), build targets, jobs and tests in YAML files. Flowman reads these specs,
resolves dependencies and executes them on Spark, including creating/migrating/destroying tables.

* Homepage: <https://flowman.io>, documentation: <https://docs.flowman.io> (sources in `docs/`)
* License: Apache 2.0, every source file carries the Apache license header (`Copyright (C) <year> The Flowman Authors`)
* Current version: see `<version>` in the root `pom.xml` (e.g. `1.3.7-SNAPSHOT` on `branch-1.3`)

## Tech Stack

* **Scala 2.12** (2.11 for Spark 2.4 / Cloudera profiles) and **Java**, compiled by `scala-maven-plugin`
  (the plain `maven-compiler-plugin` is disabled)
* **Apache Spark** 2.4 – 3.5 (default: 3.5), **Hadoop** 2.6 – 3.3 (default: 3.3), **Java 11** (Java 8 for Spark 2.4, EMR, Synapse)
* **Jackson** for YAML deserialization, **Velocity** for templating/expressions
* **ScalaTest** (`AnyFlatSpec` + `Matchers`) for tests, run via `scalatest-maven-plugin`
* **gRPC/Protobuf** for the kernel API, **Jersey** for the server, **Vue 2 + Vuetify** for the server UI
* **Sphinx** (Markdown via MyST) for the documentation

## Repository Layout

| Path | Purpose |
|------|---------|
| `flowman-spark-dependencies` | Aggregates the Spark/Hadoop dependencies for the selected profile |
| `flowman-spark-testing` | Spark test helpers (`LocalSparkSession`, `LocalTempDir`, `QueryTest`) |
| `flowman-scalatest-compat` | ScalaTest compatibility shims across Scala versions |
| `flowman-spark-extensions` | Spark extensions; version-specific code in `src/main/spark-<x.y>` |
| `flowman-common`, `flowman-common-java`, `flowman-common-jersey` | Shared utilities |
| `flowman-core` | Core model & execution engine: `model` (Mapping, Relation, Target, Job, Project, …), `execution`, `graph`, `catalog`, `types`, `templating`, `spi`, `history`, `metric`, `documentation` |
| `flowman-spec` | YAML spec classes (`*Spec`) and built-in implementations of mappings, relations, targets, assertions, …; Jackson-version-specific code in `src/main/jackson-<x.y>` |
| `flowman-plugins/*` | Optional plugins (avro, aws, azure, delta, kafka, jdbc drivers like postgresql/mariadb/mysql/mssqlserver/oracle, sftp, swagger/openapi, json, impala, trino; `hbase` is disabled) |
| `flowman-testing` | Testing API for unit-testing Flowman projects |
| `flowman-tools` | CLI tools: `flowexec` (`tools/exec`) and `flowshell` (`tools/shell`) |
| `flowman-kernel-api` | gRPC/Protobuf protocol definitions (`src/main/proto`) |
| `flowman-kernel-common`, `flowman-kernel-tools` | Java kernel client code and CLI tools |
| `flowman-kernel-server` | Kernel server (Scala) |
| `flowman-server`, `flowman-server-ui` | History server (Scala) and its web UI (Vue, built with npm during the Maven build) |
| `flowman-yaml-schema` | Generated JSON schema for YAML editor support |
| `flowman-parent` | Parent BOM for Flowman projects |
| `flowman-dist` | Distribution `tar.gz` (start scripts in `flowman-dist/bin`, e.g. `flowexec`, `flowshell`) |
| `docker` | Docker image `dimajix/flowman` |
| `docs` | User documentation (Sphinx). Spec reference lives in `docs/spec/<entity>/<kind>.md` |
| `examples` | Example Flowman projects (e.g. `weather`) |
| `tests` | Docker-compose based integration tests against real systems (databases, EMR, Synapse, …) |
| `devtools` | Release scripts |

## Building

```shell
# Full build without Docker image and GPG signing (what CI does)
mvn clean install -Ddockerfile.skip -Dgpg.skip

# Faster: skip tests
mvn clean install -DskipTests -Ddockerfile.skip -Dgpg.skip

# Build a single module (and the modules it depends on)
mvn install -pl flowman-spec -am -DskipTests -Ddockerfile.skip -Dgpg.skip

# Build for a different Spark/Hadoop combination
mvn clean install -Pspark-3.4 -Phadoop-3.3 -DskipTests -Ddockerfile.skip -Dgpg.skip
```

Notes:
* Requires JDK 11, Maven, npm (for `flowman-server-ui`), optionally Docker. The Scala compiler is configured with 4 GB heap.
* Available profiles: `spark-2.4` … `spark-3.5`, `hadoop-2.6` … `hadoop-3.3`, `CDH-6.3`, `CDP-7.1`, `CDP-7.1-spark-3.2`,
  `CDP-7.1-spark-3.3`, `EMR-6.12`, `synapse-3.3`. See `BUILDING.md`.
* A full build with tests takes a long time. Prefer building/testing only the affected modules.

## Testing

```shell
# Run all tests of one module (dependencies must be installed in the local Maven repo)
mvn test -pl flowman-spec -Ddockerfile.skip -Dgpg.skip

# Run a single ScalaTest suite
mvn test -pl flowman-spec -Dsuites='com.dimajix.flowman.spec.mapping.DistinctMappingTest' -Ddockerfile.skip -Dgpg.skip
```

* Tests live in `src/test/scala` next to the code, named `<ClassName>Test.scala`, written as
  `class FooTest extends AnyFlatSpec with Matchers [with LocalSparkSession]`.
* Typical test pattern for spec classes: parse a YAML snippet via `Module.read.string(spec).toProject("project")`,
  create a `Session.builder().withSparkSession(spark).build()`, instantiate via the context and assert on the result.
  Call `session.shutdown()` at the end.
* Tests use a local Spark session; Spark UI is disabled and `spark.sql.shuffle.partitions=8` (see root `pom.xml`).
* The `tests/` directory contains manual integration tests that need Docker and external services — do not run them as
  part of normal verification unless explicitly asked.

## Architecture & Extension Points

* **Model vs. Spec:** `flowman-core` defines runtime model traits (e.g. `Mapping`, `Relation`, `Target`, usually via a
  `Base*` class). `flowman-spec` defines Jackson-deserializable `*Spec` classes which `instantiate(context, ...)` the
  runtime objects. Runtime instances are typically immutable `final case class`es whose first argument is
  `instanceProperties: <Entity>.Properties`.
* **Expressions:** Every string property coming from YAML must be evaluated via `context.evaluate(...)` in
  `instantiate`, so that `${...}` Velocity expressions and environment variables work.
* **Registering a new kind:**
  * Built-in types in `flowman-spec` are registered in the `@JsonSubTypes` list of the corresponding base spec
    (e.g. `MappingSpec.scala`: `new JsonSubTypes.Type(name = "distinct", value = classOf[DistinctMappingSpec])`).
  * Plugins register types via annotations such as `@RelationType(kind="kafka")`, `@MappingType(...)`,
    `@TargetType(...)` (Java annotations in `com.dimajix.flowman.spec.annotation`), discovered by the `ClassAnnotationScanner` and the `*SpecAnnotationHandler`s
    (registered in `META-INF/services/com.dimajix.flowman.spi.ClassAnnotationHandler`).
  * Each plugin ships a `src/main/resources/plugin.yml` listing its jars.
* **Mappings** implement `inputs`, `execute(execution, tables): Map[String, DataFrame]` (default output `"main"`) and
  `describe(execution, input): Map[String, StructType]` for schema inference without executing Spark jobs.
* **Version-specific code:** If Spark or Jackson APIs differ between versions, put the variant into the
  `src/main/spark-<x.y>` / `src/main/jackson-<x.y>` source folders for *every* supported version instead of using
  reflection hacks in shared code.

## Coding Conventions

* Follow `.editorconfig`: 4 spaces for Scala/Java, 2 spaces for YAML/JS/JSON/Vue/Markdown, LF line endings, max line
  length 120 for Scala.
* Adapt to the style of the surrounding code; camelCase naming.
* Scala import order (see `.idea/codeStyles/Project.xml`), each group separated by a blank line:
  `java.*` → `scala.*` → all other imports → `com.dimajix.jackson.*` → `com.dimajix.*`. One import per line
  (no wildcard/grouped imports in new code unless the file already does so). Two blank lines after imports.
* No `println` — use SLF4J logging (`private val logger = LoggerFactory.getLogger(getClass)`).
* Keep code compatible with **Scala 2.11 and 2.12** and all supported Spark versions (e.g. avoid Scala 2.13-only
  collection APIs).
* Add the Apache license header to every new source file.
* Write unit tests for new functionality.
* **Backwards compatibility is essential:** never change or remove existing properties from YAML specs. Existing
  projects written for older Flowman versions must keep working. New properties must be optional with sensible defaults.

## Documentation & Changelog

* When adding or changing a spec feature (new mapping/relation/target kind, new property), update the matching page in
  `docs/spec/...` (and the index/toctree if a new page is added).
* Add a short entry to the topmost version section in `CHANGELOG.md` (and `docs/releases.md` where applicable).
* Docs build: `cd docs && make clean html` (requires Python 3.10+ and Poetry).

## Git & Repository Hygiene

* Main branch is `main`; maintenance branches are `branch-<major>.<minor>` (e.g. `branch-1.3`).
* Reference GitHub issues in commit messages with `Fixes #<issue>` where applicable.
* Never commit local/generated artifacts or secrets: `target/`, `metastore_db/`, `spark-warehouse/`, `derby.log`,
  `.flattened-pom.xml`, `.idea/`, credential files (e.g. `azure-creds.txt`) or local config files in the repo root.
