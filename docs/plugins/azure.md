# Azure Plugin

The Azure plugin mainly provides the ADLS (Azure DataLake Filesystem) and ABS (Azure Blob Filesystem) to be used 
as the storage layer.


## Activation

The plugin can be easily activated by adding the following section to the [default-namespace.yml](../spec/namespace.md)
```yaml
plugins:
  - flowman-azure 
```
