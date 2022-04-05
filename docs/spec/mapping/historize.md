# Historize Mapping
The `historize` mapping creates a historized output for entities by adding new columns containing the valid time range.
The column names are specified via `validFromColumn` and `validToColumn`.

## Example
```yaml
mappings:
  card_history:
    kind: historize
    input: card
    keyColumns: id
    timeColumn: ts
    validFromColumn: valid_from
    validToColumn: valid_until
    columnInsertPosition: beginning
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `historize`

* `broadcast` **(optional)** *(type: boolean)* *(default: false)*: 
Hint for broadcasting the result of this mapping for map-side joins.

* `cache` **(optional)** *(type: string)* *(default: NONE)*:
Cache mode for the results of this mapping. Supported values are
  * `NONE`
  * `DISK_ONLY`
  * `MEMORY_ONLY`
  * `MEMORY_ONLY_SER`
  * `MEMORY_AND_DISK`
  * `MEMORY_AND_DISK_SER`

* `input` **(mandatory)** *(type: string)*:
Specifies the name of the input mapping to be historized.

* `keyColumns` **(mandatory)** *(type: string)*:
Specifies the columns that make up the primary key of the entities to be historized.

* `timeColumn` **(mandatory)** *(type: string)*:
Specifies the name of the input column which contains the date and/or time. The value of this column
will be provided in the historized output in the `validFromColumn` and `validToColumn`.

* `validFromColumn` **(mandatory)** *(type: string)* *(default: `valid_from`):
Name of the output column where the start date should be recorded to.

* `validToColumn` **(mandatory)** *(type: string)* *(default: `valid_to`):
Name of the output column where the end date should be recorded to.

* `filter` **(optional)** *(type: string)* *(default: empty)*:
  An optional SQL filter expression that is applied *after* the transformation itself.


## Outputs
* `main` - the only output of the mapping

