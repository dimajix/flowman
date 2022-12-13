# Console Target

The `console` target will simply display some records on stdout.

## Example
```yaml
targets:
  measurements-dump:
    kind: console
    input:
      kind: mapping
      mapping: measurements
    limit: 100
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `console`
* `description` **(optional)** *(type: string)*:
Optional descriptive text of the build target
* `input` **(mandatory)** *(type: dataset)*:
Specified the [dataset](../dataset/index.md) containing the records to be dumped 
* `limit` **(optional)** *(type: integer)* *(default: 100)*: 
Specified the number of records to be displayed


## Supported Execution Phases
* `BUILD` - The target will only be executed in the *build* phase

Read more about [execution phases](../../concepts/lifecycle.md).


## Dirty Condition
A `console` target is always dirty for the `BUILD` execution phase.
