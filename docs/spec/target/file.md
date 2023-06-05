# File Target

A target for writing files into a shared file system like HDFS or S3. In most cases you should prefer using a
[File Relation](../relation/file.md) together with a [Relation Target](relation.md) instead of using a file target.

## Example:
```yaml
targets:
  csv_export:
    kind: file
    mapping: some_mapping
    format: "csv"
    location: "${export_dir}"
    mode: overwrite
    parallelism: 32
    rebalance: true
    options:
      delimiter: ","
      quote: "\""
      escape: "\\"
      header: "true"
      compression: "gzip"
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `file`

* `description` **(optional)** *(type: string)*:
  Optional descriptive text of the build target

* `mapping` **(optional)** *(type: string)*: 
Specifies the name of the input mapping to be written

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

* `rebalance` **(optional)** *(type: boolean)* *(default=false)*:
Enables rebalancing the size of all partitions by introducing an additional internal shuffle operation. Each partition 
and output file will contain approximately the same number of records. The default value is controlled by the
Flowman config variable `floman.default.target.rebalance`.


## Supported Phases
* `CREATE` - creates the target directory
* `BUILD` - build the target files containing records
* `VERIFY` - verifies that the target file exists
* `TRUNCATE` - removes the target file, but keeps the directory
* `DESTROY` - recursively removes the target directory and all files inside

Read more about [execution phases](../../concepts/lifecycle.md).


## Provided Metrics
The relation target also provides some metric containing the number of records written:

* Metric `target_records` with the following set of attributes
    - `name` - The name of the target
    - `category` - Always set to `target`
    - `kind` - Always set to `file`
    - `namespace` - Name of the namespace (typically `default`)
    - `project` - Name of the project
    - `version` - Version of the project

See [Execution Metrics](../../cookbook/execution-metrics.md) for more information how to use these metrics.
