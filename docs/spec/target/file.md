# File Target

A target for writing files into a shared filesystem like HDFS or S3. In most cases you should prefer using a 
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

* `mapping` **(optional)** *(type: string)*: 
Specifies the name of the input mapping to be written

* `mode` **(optional)** *(type: string)* *(default=overwrite)*: 
Specifies the behavior when data or table or partition already exists. Options include:
  * `overwrite`: overwrite the existing data.
  * `append`: append the data.
  * `ignore`: ignore the operation (i.e. no-op).
  * `error` or `errorifexists`: throw an exception at runtime . 

* `partition` **(optional)** *(type: map:string)* *(default=empty)*:

* `parallelism` **(optional)** *(type: integer)* *(default=16)*:
This specifies the parallelism to be used when writing data. The parallelism equals the number
of files being generated in HDFS output and also equals the maximum number of threads that
are used in total in all Spark executors to produce the output.

* `rebalance` **(optional)** *(type: bool)* *(default=false)*:
Enables rebalancing the size of all partitions by introducing an additional internal shuffle
operation. Each partition will contain approximately the same number of records.


## Supported Phases
* `CREATE`
* `BUILD`
* `VERIFY`
* `TRUNCATE`
* `DESTROY`
