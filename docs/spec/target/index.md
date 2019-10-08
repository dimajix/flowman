---
layout: page
title: Flowman Output Specifications
permalink: /spec/target/index.html
---
# Flowman Output Specification


## Output Types

* [`blackhole`](blackhole.html): 
Use a [Blackhole Target](blackhole.html) to execute mappings without using the results

* [`console`](console.html): 
Use [Console Target](console.html) to dump the contents of a mapping onto the console

* [`copy`](copy.html): 

* [`copyFile`](copy-file.html): 

* [`count`](count.html): 

* [`getFile`](get-file.html): 

* [`hiveDatabase`](hive-database.html): 

* [`local`](local.html): 
Use the [Local Target](local.html) to write into a local CSV file

* [`relation`](relation.html): 
Use the [Relation Target](relation.html) to write into a relation

## Metrics

For each target Flowman provides the following execution metric:
* metric: "target_runtime"
* labels: 
  * category: "target"
  * kind:
  * namespace: 
  * project: 
