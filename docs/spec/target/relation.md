---
layout: page
title: Flowman Relation Target
permalink: /spec/target/relation.html
---
# Relation Target

The relation target operation probably is the most important and common output operation. It 
writes the result of a mapping to a relation. The relation then is responsible for specifying
the physical location or connection, the format and so on.

## Example

```
targets:
  stations:
    kind: relation
    input: stations-mapping
    target: stations-relation
    mode: overwrite
    partition:
      processing_date: "${processing_date}"
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `relation`

* `input` **(mandatory)** *(type: string)*: 
Specifies the name of the input mapping to be written

* `target` **(mandatory)** *(type: string)*: 
Specifies the name of the relation to write to

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


## Description
