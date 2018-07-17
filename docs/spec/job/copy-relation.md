---
layout: page
title: Flowman Copy Relation
permalink: /spec/job/copy-relation.html
---
# Copy Relation

A `copy-relation` task will copy partitions from one relation to another relation. This is very
useful if used to upload local file based data directly into a real data source.

## Example
```
jobs:
  main:
    tasks:
     - kind: copy-relation
       source: source_relation
       sourcePartitions:
         partition_column: part_value
       target: target_relation
       targetPartition:
         partition_column: part_value
       mode: overwrite
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `copy-relation`

* `source` **(mandatory)** *(type: string)*:
This specifies the name of the source relation.

* `sourcePartitions` **(optional)** *(type: map:string)* *(default: empty)*:
In order to read only specific partitions of a partitioned relation, you can specify the
partition columns with values which should be read

* `target` **(mandatory)** *(type: string)*:
This specifies the name of the source relation.

* `targetPartition` **(optional)** *(type: map:string)* *(default: empty)*:
In order to write into a partitioned relation, you can specify the partition columns with values 
where data should be written to

* `mode` **(optional)** *(type: string)* *(default: overwrite)*:
Specifies the behavior when data or table or partition already exists. Options include:
  * `overwrite`: overwrite the existing data.
  * `append`: append the data.
  * `ignore`: ignore the operation (i.e. no-op).
  * `error` or `errorifexists`: throw an exception at runtime . 


## Description
