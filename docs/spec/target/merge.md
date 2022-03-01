# Merge Target

The `merge` target is used to perform flexible merge operations as a superset of simple upsert operations, which are
already supported via the [`relation`](relation.md) target.

## Example

```yaml
targets:
  stations:
    kind: merge
    mapping: stations_mapping
    relation: stations
    parallelism: 32
    rebalance: true
    mergeKey: 
      - usaf
      - wban
    clauses:
      - condition: "source.action = 'INSERT'"
        action: insert
      - condition: "source.action = 'DELETE'"
        action: delete
      - condition: "source.action = 'UPDATE'"
        action: update

relations:
  stations:
    kind: deltaFile
    location: "$basedir/stations/"
    schema:
      kind: avro
      file: "${project.basedir}/schema/stations.avsc"
```

```yaml
targets:
  stations:
    kind: merge
    mapping: stations_mapping
    relation: stations
    parallelism: 32
    rebalance: true
    condition: "source.usaf = target.usaf AND source.wban = target.wban" 
    clauses:
      - condition: "source.op = 'INSERT'"
        action: insert
        columns:
          id: "source.id"
          name: "upper(source.name)"
      - condition: "source.op = 'DELETE'"
        action: delete
      - condition: "source.op = 'UPDATE'"
        action: update
        columns:
          name: "upper(source.name)"

relations:
  stations:
    kind: deltaFile
    location: "$basedir/stations/"
    schema:
      kind: avro
      file: "${project.basedir}/schema/stations.avsc"
```


## Fields

* `kind` **(mandatory)** *(type: string)*: `merge`

* `description` **(optional)** *(type: string)*:
  Optional descriptive text of the build target

* `mapping` **(optional)** *(type: string)*:
  Specifies the name of the input mapping to be written

* `relation` **(mandatory)** *(type: string)*:
  Specifies the name of the relation to write to

* `mergeKey` **(optional)** *(type: list[string])*:
  Specifies the list of columns used for matching the new source columns with existing target columns.

* `condition` **(optional)** *(type: string)*:
  As an alternative to `mergeKey` you can also explicitly specify an arbitrary merge condition. You should use
  the prefixes `source.` to refer to the incoming source records (from the mapping) and `target.` to refer to
  columns in the target table. **Note that this condition is executed on the SQL server side, so you can only use
  SQL functions available on the server**

* `clauses` **(required)** *(type: list)*:

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


## Supported Execution Phases
* `CREATE` - This will create the target relation or migrate it to the newest schema (if possible).
* `BUILD` - This will write the output of the specified mapping into the relation. If no mapping is specified, nothing
  will be done.
* `VERIFY` - This will verify that the relation (and any specified partition) actually contains data.
* `TRUNCATE` - This removes the contents of the specified relation. The relation itself will not be removed (for example
  if the relation refers to a Hive table)
* `DESTROY` - This drops the relation itself and all its content.

Read more about [execution phases](../../lifecycle.md).


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
