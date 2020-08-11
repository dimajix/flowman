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
* `input` **(mandatory)** *(type: dataset)*:
Specified the [dataset](../dataset/index.md) containing the records to be dumped 
* `limit` **(optional)** *(type: integer)* *(default: 100)*: 
Specified the number of records to be displayed

## Supported Phases
* `BUILD` - The target will only be executed in the *build* phase
