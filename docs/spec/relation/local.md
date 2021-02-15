# Local Relations
In addition to working with file relations backed up by Hadoop compatible file systems (HDFS,
S3, ...), Flowman also supports the local file system as backend for working with files. The
implementation is independant of the normal Apache Spark data sources, therefore only a very
limited set of file formats are supported.

## Example
```yaml
relations:
  local:
    kind: local
    location: $outputPath
    pattern: data.csv
    format: csv
    schema:
      kind: inline
      fields:
        - name: str_col
          type: string
        - name: int_col
          type: integer
```

## Fields
 * `kind` **(mandatory)** *(string)*: `local`
 
 * `schema` **(optional)** *(schema)* *(default: empty)*: 
 Explicitly specifies the schema of the local relation.
 
 * `description` **(optional)** *(string)* *(default: empty)*:
 A description of the relation. This is purely for informational purpose.
 
 * `options` **(optional)** *(map:string)* *(default: empty)*:
 All options are passed directly to the reader/writer backend and are specific to each
 supported format.
 
 * `format` **(optional)** *(string)* *(default: csv)*:
 This specifies the file format to use. Since the implementation of local file formats
 is separate from Apache Spark, only a limited number of formats are currently supported.

 * `location` **(mandatory)** *(string)*:
  This field specifies the storage location in the local file system. If the data 
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

### CSV

## Partitioning
