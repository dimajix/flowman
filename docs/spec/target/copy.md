# Copy Target

The `copy` target can be used to copy contents of one data set to another. A dataset can be 'file', 'mapping', 'relation'
or other supported types.

## Example

```yaml
targets:
  stations:
    kind: copy
    source:
      kind: relation
      relation: weather_records
      partition:
        processing_date: "${processing_date}"
    target:
      kind: file
      format: csv
      location: "/landing/weather/data"
    schema:
      format: spark
      file: "/landing/weather/schema.json"
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `copy`

* `description` **(optional)** *(type: string)*:
  Optional descriptive text of the build target

* `source` **(mandatory)** *(type: dataset)*: 
Specifies the source data set to be copied from.

* `target` **(mandatory)** *(type: dataset)*: 
Specifies the target data set to be copied to.

* `schema` **(optional)**:
Optionally specify a schema file to be written to. This file will be created in the `build` phase. The schema contains
two sub elements `format` and `file`.  

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


## Supported Phases
* `BUILD` - The *build* phase will perform the copy operation
* `VERIFY` - The *verify* phase will ensure that the target exists
* `TRUNCATE` - The *truncate* phase will remove the target
* `DESTROY` - The *destroy* phase will remove the target
