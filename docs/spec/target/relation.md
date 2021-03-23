# Relation Target

The `relation` target operation probably is the most important and common output operation. It 
writes the result of a mapping to a relation. The relation then is responsible for specifying
the physical location or connection, the format and so on.

## Example

```yaml
targets:
  stations:
    kind: relation
    input: stations-mapping
    target: stations-relation
    mode: overwrite
    parallelism: 32
    rebalance: true
    partition:
      processing_date: "${processing_date}"
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `relation`

* `mapping` **(optional)** *(type: string)*: 
Specifies the name of the input mapping to be written

* `relation` **(mandatory)** *(type: string)*: 
Specifies the name of the relation to write to

* `mode` **(optional)** *(type: string)* *(default=overwrite)*: 
Specifies the behavior when data or table or partition already exists. Options include:
  * `overwrite`: overwrite the existing data.
  * `append`: append the data.
  * `ignore`: ignore the operation (i.e. no-op).
  * `error` or `errorifexists`: throw an exception at runtime . 
The default value is controlled by the Flowman config variable `floman.default.target.outputMode`.

* `partition` **(optional)** *(type: map:string)* *(default=empty)*:

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


## Supported Phases
* `CREATE` - This will create the target relation or migrate it to the newest schema (if possible).
* `BUILD` - This will write the output of the specified mapping into the relation. If no mapping is specified, nothing
 will be done. 
* `VERIFY`
* `TRUNCATE` - This removes the contents of the specified relation. The relation itself will not be removed (for example
if the relation refers to a Hive table)
* `DESTROY` - This drops the relation itself and all its content.


## Provided Metrics
The relation target also provides some metric containing the number of records written:

* Metric `target_records` with the following set of attributes
  - `name` - The name of the target
  - `category` - Always set to `target`
  - `kind` - Always set to `relation`
  - `namespace` - Name of the namespace (typically `default`)
  - `project` - Name of the project
  - `version` - Version of the project
