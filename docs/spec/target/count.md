# Count Target

## Example
```
targets:
  measurements-count:
    kind: count
    mapping: measurements
```

## Fields
 * `kind` **(mandatory)** *(string)*: `count`
 * `description` **(optional)** *(type: string)*:
  Optional descriptive text of the build target
 * `mapping` **(mandatory)** *(string)*:
 Specifies the name of the input mapping to be counted


## Supported Execution Phases
* `BUILD` - Counting records of a mapping will be executed as part of the `BUILD` phase

Read more about [execution phases](../../concepts/lifecycle.md).
