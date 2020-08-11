# Mapping Dataset

A *mapping dataset* represents the records as produced by a named mapping. Note that this dataset only supports read
operations, since mapping cannot perform any write operations

## Example
```yaml
kind: mapping
mapping: ${mapping}
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `relation`
* `mapping` **(mandatory)** *(type: string)*: Name of the mapping output
