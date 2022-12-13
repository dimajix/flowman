# Relation Target

The `relation` target operation probably is the most important and common output operation. It 
writes the result of a mapping to a relation. The relation then is responsible for specifying
the physical location or connection, the format and so on.

## Example

```yaml
targets:
  stations:
    kind: relation
    mapping: stations_mapping
    relation: stations
    mode: overwrite
    parallelism: 32
    rebalance: true
    partition:
      year: "${processing_date}"

relations:
  stations:
    kind: file
    format: parquet
    location: "$basedir/stations/"
    schema:
      kind: avro
      file: "${project.basedir}/schema/stations.avsc"
    partitions:
      - name: year
        type: integer
        granularity: 1
```

Since Flowman 0.18.0, you can also directly specify the relation inside the target definition. This saves you
from having to create a separate relation definition in the `relations` section. This is only recommended, if you
do not access the target relation otherwise, such that a shared definition would not provide any benefit.
```yaml
targets:
  stations:
    kind: relation
    mapping: stations_mapping
    relation:
      kind: file
      name: stations-relation
      format: parquet
      location: "$basedir/stations/"
      schema:
        kind: avro
        file: "${project.basedir}/schema/stations.avsc"
      partitions:
        - name: year
          type: integer
          granularity: 1
    mode: OVERWRITE
    buildPolicy: IF_EMPTY
    parallelism: 32
    rebalance: true
    partition:
      year: "${processing_date}"
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `relation`

* `description` **(optional)** *(type: string)*:
  Optional descriptive text of the build target.

* `mapping` **(optional)** *(type: string)*: 
Specifies the name of the input mapping to be written. When no mapping is specified, then the target will only
create or destroy the corresponding relation, but not populate it with data.

* `relation` **(mandatory)** *(type: string)*: 
Specifies the name of the relation to write to, or alternatively directly embeds the relation.

* `mode` **(optional)** *(type: string)* *(default=overwrite)*: 
Specifies the behavior when data or table or partition already exists. Options include:
  * `OVERWRITE`: overwrite the existing data. If dynamically writing to a partitioned table, all partitions will be 
    removed first.
  * `OVERWRITE_DYNAMIC` or `DYNAMIC_OVERWRITE`: overwrite the existing data. If dynamically writing to a partitioned table, only those 
    partitions with new records will be replaced
  * `APPEND`: append the data to already existing data.
  * `UPDATE` or `UPSERT`: perform upserts - not all relations support this ([JDBC](../relation/jdbcTable.md) and 
    [Delta Lake](../relation/deltaTable.md) are two examples supporting upserts).
  * `IGNORE` or `IGNORE_IF_EXISTS`: ignore the operation (i.e. no-op).
  * `ERROR` or `ERROR_IF_EXISTS`: throw an exception at runtime . 
The default value is controlled by the Flowman [config variable](../../setup/config.md) `flowman.default.target.outputMode`.

* `partition` **(optional)** *(type: map:string)* *(default=empty)*:
Specifies the partition to be written to. When the target relation has defined partitions, Flowman always supports
  writing into individual partitions. Some relation types ([`hiveTable`](../relation/hiveTable.md), 
  [`jdbc`](../relation/jdbcTable.md) and [`file`](../relation/file.md)) also support *dynamic partitioning*, where
  you do not specify an explicit target partition, but simply pass in possibly multiple different partition values
  in the data itself (i.e. the output of the `mapping` which is written to the relation).

* `parallelism` **(optional)** *(type: integer)* *(default=16)*:
This specifies the parallelism to be used when writing data. The parallelism equals the number
of files being generated in HDFS output and also equals the maximum number of threads that are used in total in all 
Spark executors to produce the output. If `parallelism` is set to zero or to a negative number, Flowman will not 
coalesce any partitions and generate as many files as Spark partitions. The default value is controlled by the
Flowman [config variable](../../setup/config.md) `floman.default.target.parallelism`.

* `rebalance` **(optional)** *(type: bool)* *(default=false)*:
Enables rebalancing the size of all partitions by introducing an additional internal shuffle operation. Each partition 
and output file will contain approximately the same number of records. The default value is controlled by the
Flowman [config variable](../../setup/config.md) `floman.default.target.rebalance`.

* `buildPolicy` **(optional)** *(type: string)* *(default=empty)*:
Specifies a build policy, which determines when the target is considered to be dirty. If no value is provided, then
Flowman will fall back to the [config variable](../../setup/config.md) `flowman.default.target.buildPolicy`. Possible values are
  - `IF_EMPTY`: The target is considered to be dirty, when it is empty.
  - `ALWAYS`: The target is always considered to be dirty.
  - `SMART`: The target is considered to be dirty, when the target partition is empty, or when the output mode is set to `APPEND` or when no partition is specified (full overwrite)
  - `COMPAT`: The target is considered to be dirty, when the target is empty, or when the output mode is set to `APPEND`.


## Description

The `relation` target will write the output of a mapping specified via the `mapping` field into the relation specified
in `relation`. If the `mapping` field is not specified, then Flowman will only perform actions for creating and removing
the relation during the `CREATE`, `TRUNCATE` and `DESTROY` phase. In this case, the `BUILD` phase is a no-op for this
target.


## Supported Execution Phases
* `CREATE` - This will create the target relation or migrate it to the newest schema (if possible).
* `BUILD` - This will write the output of the specified mapping into the relation. If no mapping is specified, nothing
 will be done. 
* `VERIFY` - This will verify that the relation (and any specified partition) actually contains data.
* `TRUNCATE` - This removes the contents of the specified relation. The relation itself will not be removed (for example
if the relation refers to a Hive table). Note for consistency reasons, the `TRUNCATE` phase will only be enabled for
relation targets with a mapping.
* `DESTROY` - This drops the relation itself and all its content.

Read more about [execution phases](../../concepts/lifecycle.md).


## Dirty Condition
Flowman will apply some logic to find out if a relation target is to be considered being *dirty* for a specific execution
phase, which means that it needs to participate in that phase. The logic depends on the execution phase as follows:
* `CREATE` - A relation target is considered to be dirty, when the relation physically does not exist, or when its
schema is not up-to-date. Then Flowman will either create the relation or perform a 
[migration](../../concepts/migrations.md).
* `BUILD` - A relation target is dirty in the `BUILD` according to the build policy.
* `VERIFY` - A relation target is always dirty during the `VERIFY` phase.
* `TRUNCATE` - A relation target is dirty in the `TRUNCATE` phase when it contains some records, which need to be removed.
* `DESTROY` - A relation target is dirty in the `TRUNCATE` phase when it physically exists and needs to be dropped.


## Provided Metrics
The relation target also provides some metric containing the number of records written:

* Metric `target_records` with the following set of attributes
  - `name` - The name of the target
  - `category` - Always set to `target`
  - `kind` - Always set to `relation`
  - `namespace` - Name of the namespace (typically `default`)
  - `project` - Name of the project
  - `version` - Version of the project

See [Execution Metrics](../../cookbook/execution-metrics.md) for more information how to use these metrics.
