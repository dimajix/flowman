
# Flowman Output Specification


## Output Types

* [`blackhole`](blackhole.md): 
Use a [Blackhole Target](blackhole.md) to execute mappings without using the results

* [`console`](console.md): 
Use [Console Target](console.md) to dump the contents of a mapping onto the console

* [`copy`](copy.md): 

* [`copyFile`](copy-file.md): 

* [`count`](count.md): 

* [`getFile`](get-file.md): 

* [`hiveDatabase`](hive-database.md): 

* [`local`](local.md): 
Use the [Local Target](local.md) to write into a local CSV file

* [`relation`](relation.md): 
Use the [Relation Target](relation.md) to write into a relation

## Metrics

For each target Flowman provides the following execution metric:
* metric: "target_runtime"
* labels: 
  * category: "target"
  * kind:
  * namespace: 
  * project: 
