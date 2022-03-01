# AWS Plugin

The AWS plugin does not provide new entity types to Flowman, but will provide compatibility with the S3 object
store to be usable as a data source or sink via the `s3a` file system.


## Activation

The plugin can be easily activated by adding the following section to the [default-namespace.yml](../spec/namespace.md)
```yaml
plugins:
  - flowman-aws 
```
