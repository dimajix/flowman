# Azure Plugin

The Azure plugin mainly provides the ADLS2 (Azure Data Lake file system) and ABS (Azure Blob Storage) to be used
as the storage layer.


## Provided Template Functions 
Since Flowman version 1.1.0, the plugin also provides the templating function `AzureKeyVault.getSecret` to access Azure
Key Vaults.


## Activation

The plugin can be easily activated by adding the following section to the [default-namespace.yml](../spec/namespace.md)
```yaml
plugins:
  - flowman-azure 
```
