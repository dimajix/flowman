# Delete File Target

## Example

```yaml
targets:
  deleteTempFiles:
    kind: deleteFile
    location: hdfs:///tmp/my-location
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `deleteFile`

* `description` **(optional)** *(type: string)*:
  Optional descriptive text of the build target

* `location` **(mandatory)** *(type: string)*: 


## Supported Execution Phases
* `BUILD` - This will remove the specified location

Read more about [execution phases](../../concepts/lifecycle.md).
