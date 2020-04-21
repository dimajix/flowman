# Flowman Count Target

## Example
```
targets:
  measurements-count:
    kind: count
    mapping: measurements
```

## Fields
 * `kind` **(mandatory)** *(string)*: `count`
 * `mapping` **(mandatory)** *(string)*:
 Specifies the name of the input mapping to be counted


## Supported Phases
* `BUILD`
