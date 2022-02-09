# Blackhole Target

A *blackhole target* simply materializes all records of a mapping, but immediately discards them. This can be 
useful for some test scenarios but probably is not worth much in a real production environment.
 

## Example

```yaml
targets:
  blackhole:
    kind: blackhole
    mapping: some_mapping
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `blackhole`

* `description` **(optional)** *(type: string)*: 
Optional descriptive text of the build target

* `mapping` **(mandatory)** *(type: string)*: 
Specifies the name of the mapping output to be materialized


## Supported Phases
* `BUILD` - In the build phase, all records of the specified mapping will be materialized
