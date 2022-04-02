# File Relations

File relations are among the most simple relation types. They refer to data stored in individual files, typically on
a distributed and shared file system or object store like Hadoop HDFS or S3.

## Example
```yaml
relations:
  csv_export:
    kind: file
    # Specify the file format to use
    format: "csv"
    # Specify the base directory where all data is stored. This location does not include the partition pattern
    location: "${export_dir}"
    # You could specify the pattern how to identify files and/or partitions. This pattern is relative to the `location`.
    # Actually, it is highly recommended NOT to explicitly specify a partition pattern for outgoing relations
    # and let Spark generate this according to the Hive standard.
    pattern: "${export_pattern}"
    # Set format specific options
    options:
      delimiter: ","
      quote: "\""
      escape: "\\"
      header: "true"
      compression: "gzip"
    # Add partition column, which can be used in the `pattern`
    partitions:
      - name: datetime
        type: timestamp
        granularity: "P1D"
    # Specify an optional schema here. It is always recommended to explicitly specify a schema for every relation
    # and not just let data flow from a mapping into a target.
    schema:
      kind: inline
      fields:
        - name: country
          type: STRING
        - name: min_wind_speed
          type: FLOAT
        - name: max_wind_speed
          type: FLOAT
```

## Fields
 * `kind` **(mandatory)** *(string)*: `file`
 
 * `schema` **(optional)** *(schema)* *(default: empty)*: 
 Explicitly specifies the schema of the JDBC source. Alternatively Flowman will automatically
 try to infer the schema if the underlying file format supports this.

 * `description` **(optional)** *(string)* *(default: empty)*:
 A description of the relation. This is purely for informational purpose.
 
 * `options` **(optional)** *(map:string)* *(default: empty)*:
 Options are passed directly to Spark for reading and/or writing data. Options are specific
 to the selected file format. Best is to refer to the Apache Spark Documentation.
 
 * `format` **(optional)** *(string)* *(default: csv)*:
 This specifies the file format to use. All formats supported by Apache Spark can be used,
 for example `csv`, `parquet`, `orc`, `avro` and `json`
  
 * `location` **(mandatory)** *(string)*:
 This field specifies the storage location in the Hadoop compatible file system. If the data 
 source is partitioned, this should  specify only the root location below which partition 
 directories are created.
 
 * `partitions` **(optional)** *(list:partition)* *(default: empty)*:
 In order to use partitioned file based data sources, you need to define the partitioning columns. Each partitioning 
   column has a name and a type and optionally a granularity. Normally the partition columns are separate from the
   schema, but you *may* also include the partition column in the schema, although this is not considered to be best 
   practice. But it turns out to be quite useful in combination with dynamically writing to multiple partitions.

 * `pattern` **(optional)** *(string)* *(default: empty)*:
 This field specifies the directory and/or file name pattern to access specific partitions. 
 Please see the section [Partitioning](#Partitioning) below. 


## Automatic Migrations
The `file` relation does not support any automatic migration like adding/removing columns. 


## Schema Conversion
The `file` relation fully supports automatic schema conversion on input and output operations as described in the
corresponding section of [relations](index.md).


## Output Modes

### Batch Writing
The `file` relation supports the following output modes in a [`relation` target](../target/relation.md):

| Output Mode         | Supported | Comments                                                            |
|---------------------|-----------|---------------------------------------------------------------------|
| `errorIfExists`     | yes       | Throw an error if the files already exists                          |
| `ignoreIfExists`    | yes       | Do nothing if the files already exists                              |
| `overwrite`         | yes       | Overwrite the whole location or the specified partitions            |
| `overwrite_dynamic` | yes       | Overwrite only partitions dynamically determined by the data itself |
| `append`            | yes       | Append new records to the existing files                            |
| `update`            | no        | -                                                                   |

### Stream Writing
In addition to batch writing, the file relation also supports stream writing via the
[`stream` target](../target/stream.md) with the following semantics:

| Output Mode | Supported | Comments                                                                      |
|-------------|-----------|-------------------------------------------------------------------------------|
| `append`    | yes       | Append new records from the streaming process once they don't change any more |
| `update`    | yes       | Append records every time they are updated                                    |
| `complete`  | no        | -                                                                             |


## Remarks

When using `file` relations as data sinks in a [`relation` target](../target/relation.md), then Flowman will manage the
whole lifecycle of the directory for you. This means that
* The directory specified in `location` will be created during `create` phase
* The directory specified in `location` will be populated with records or partitioning subdirectories will be added 
  during `build` phase
* The directory specified in `location` will be truncated or individual partitions will be dropped during `clean` phase
* The directory specified in `location` tables will be removed during `destroy` phase

### Schema Inference

Note that Flowman will rely on schema inference in some important situations, like [mocking](mock.md) and generally
for describing the schema of a relation. This might create unwanted connections to the physical data source,
particular in case of self-contained tests. To prevent Flowman from creating a connection to the physical data
source, you simply need to explicitly specify a schema, which will then be used instead of the physical schema
in all situations where only schema information is required.

### Partitioning

Flowman also supports partitioning, i.e. written to different subdirectories. You can explicitly specify a *partition
pattern* via the `pattern` field, but it is highly recommended to NOT explicitly set this field and let Spark manage
partitions itself. This way Spark can infer partition values from directory names and will also list directories more
efficiently.

### Writing to Dynamic Partitions

Beside explicitly writing to a single Hive partition, Flowman also supports to write to multiple partitions where
the records need to contain values for the partition columns. This feature cannot be combined with explicitly specifying
a value for the file `pattern`, instead the standard Hive directory pattern for partitioned data will be used (i.e. 
`location/partition=value/`).


### Supported File Format

File relations support all file formats also supported by Spark. This includes simple text files, CSV files,
Parquet files, ORC files and Avro files. Each file format provides its own additional settings which can be specified
in the `options` section.

### Text

### CSV

### JSON

### Parquet

### ORC

### Avro
