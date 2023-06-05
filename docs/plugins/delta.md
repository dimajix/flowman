# Delta Lake Plugin

The `delta` plugin provide read/write support for [Delta Lake](https://delta.io) tables.

Note that this plugin is only provided for Spark 3.0+. For Spark 2.4, the plugin does not contain the Flowman relations
below, you might still be able to use it using a [`generic` relation](../spec/relation/generic.md) or
[`file` relation](../spec/relation/file.md), but this is not officially supported, and we strongly recommend moving
to Spark 3.0+.


## Provided Entities
* [`deltaTable` relation](../spec/relation/deltaTable.md)
* [`deltaFile` relation](../spec/relation/deltaFile.md)
* [`deltaVacuum` target](../spec/target/deltaVacuum.md)


## Activation

The plugin can be easily activated by adding the following section to the [default-namespace.yml](../spec/namespace.md)
```yaml
plugins:
  - flowman-delta 
```
