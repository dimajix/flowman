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
    # Specify the pattern how to identify files and/or partitions. This pattern is relative to the `location`
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
      kind: embedded
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
 In order to use partitioned file based data sources, you need to define the partitioning
 columns. Each partitioning column has a name and a type and optionally a granularity.
 
 * `pattern` **(optional)** *(string)* *(default: empty)*:
 This field specifies the directory and/or file name pattern to access specific partitions. 
 Please see the section [Partitioning](#Partitioning) below. 


## Description

## Supported File Format

File relations support all file formats also supported by Spark. This includes simple text files, CSV files,
Parquet files, ORC files and Avro files. Each file format provides its own additional settings which can be specified
in the `options` section.

### Text

### CSV

### JSON

### Parquet

### ORC

### Avro

## Partitioning
