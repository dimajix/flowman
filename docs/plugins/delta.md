# DeltaLake Plugin

The `delta` plugin provide read/write support for [DeltaLake](https://delta.io) tables.

Note that this plugin is only provided for Spark 3.0+. For Spark 2.4, the plugin does not contain the Flowman relations
below, you might still be able to use it using a [`generic` relation](../spec/relation/generic.md) or
[`file` relation](../spec/relation/file.md), but this is not officially supported and we strongly recommend to
move to Spark 3.0+.


## Provided Entities
* [`deltaTable` relation](../spec/relation/deltaTable.md)
* [`deltaFile` relation](../spec/relation/deltaFile.md)
