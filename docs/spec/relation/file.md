---
layout: page
title: Flowman File Relation
permalink: /spec/relation/file.html
---
# File Relations

## Example
```
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

### CSV

### JSON

### Parquet

### ORC

### Avro

## Partitioning
