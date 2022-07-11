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
    mode: overwrite
    parallelism: 32
    rebalance: true
    partition:
      year: "${processing_date}"
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `relation`

* `description` **(optional)** *(type: string)*:
  Optional descriptive text of the build target

* `mapping` **(optional)** *(type: string)*: 
Specifies the name of the input mapping to be written

* `relation` **(mandatory)** *(type: string)*: 
Specifies the name of the relation to write to, or alternatively directly embeds the relation.

* `mode` **(optional)** *(type: string)* *(default=overwrite)*: 
Specifies the behavior when data or table or partition already exists. Options include:
  * `overwrite`: overwrite the existing data. If dynamically writing to a partitioned table, all partitions will be 
    removed first.
  * `overwrite_dynamic`: overwrite the existing data. If dynamically writing to a partitioned table, only those 
    partitions with new records will be replaced
  * `append`: append the data.
  * `update`: perform upserts - not all relations support this ([JDBC](../relation/jdbcTable.md) and 
    [Delta Lake](../relation/deltaTable.md) are two examples supporting upserts).
  * `ignore`: ignore the operation (i.e. no-op).
  * `error` or `errorifexists`: throw an exception at runtime . 
The default value is controlled by the Flowman config variable `flowman.default.target.outputMode`.

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
Flowman config variable `floman.default.target.parallelism`.

* `rebalance` **(optional)** *(type: bool)* *(default=false)*:
Enables rebalancing the size of all partitions by introducing an additional internal shuffle operation. Each partition 
and output file will contain approximately the same number of records. The default value is controlled by the
Flowman config variable `floman.default.target.rebalance`.


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


## Provided Metrics
The relation target also provides some metric containing the number of records written:

* Metric `target_records` with the following set of attributes
  - `name` - The name of the target
  - `category` - Always set to `target`
  - `kind` - Always set to `relation`
  - `namespace` - Name of the namespace (typically `default`)
  - `project` - Name of the project
  - `version` - Version of the project

See [Execution Metrics](../../cookbook/metrics.md) for more information how to use these metrics.
