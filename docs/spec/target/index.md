# Flowman Build Target Specification

From a top level perspective, Flowman works like a build tool like make or maven. Of course in contrast to classical
build tools, the project specification in Flowman also contains the logic to be build (normally that is separated
in source code files which get compiles or otherwise processed with additional tools).

Each target supports at least some [build phases](../../lifecycle.md)


## Target Types

Flowman supports different target types, each used for a different kind of a physical entity or build recipe.

* [`blackhole`](blackhole.md): 
Use a [Blackhole Target](blackhole.md) to execute mappings without using the results

* [`console`](console.md): 
Use [Console Target](console.md) to dump the contents of a mapping onto the console

* [`copy`](copy.md): 

* [`copyFile`](copy-file.md): 

* [`count`](count.md): 

* [`file`](file.md): 
Use the [File Target](file.md) to write to files in a shared filesystem like HDFS or S3.

* [`getFile`](get-file.md): 

* [`hiveDatabase`](hive-database.md): 

* [`local`](local.md): 
Use the [Local Target](local.md) to write into a local CSV file

* [`putFile`](put-file.md): 

* [`relation`](relation.md): 
Use the [Relation Target](relation.md) to write into a relation


## Metrics

For each target Flowman provides the following execution metric:
* `metric`: "target_runtime"
* labels: 
  * `category`: "target"
  * `kind`: The kind of the target
  * `namespace`: The name of the namespace
  * `project`: The name of the project 
