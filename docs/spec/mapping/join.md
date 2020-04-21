
# Join Mapping

## Example
```
mappings:
  enriched_transactions:
    kind: join
    inputs:
      - transactions
      - enrichment
    expression: "transactions.ext_id = enrichment.id"
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `join`

* `broadcast` **(optional)** *(type: boolean)* *(default: false)*: 
Hint for broadcasting the result of this mapping for map-side joins.

* `cache` **(optional)** *(type: string)* *(default: NONE)*:
Cache mode for the results of this mapping. Supported values are
  * `NONE` - Disables caching of teh results of this mapping
  * `DISK_ONLY` - Caches the results on disk
  * `MEMORY_ONLY` - Caches the results in memory. If not enough memory is available, records will be uncached.
  * `MEMORY_ONLY_SER` - Caches the results in memory in a serialized format. If not enough memory is available, records will be uncached.
  * `MEMORY_AND_DISK` - Caches the results first in memory and then spills to disk.
  * `MEMORY_AND_DISK_SER` - Caches the results first in memory in a serialized format and then spills to disk.

* `inputs` **(mandatory)** *(type: list:string)*:
Specifies the names of the input mappings to be filtered.

* `expression` **(optional)** *(type: string)* *(default: empty)*:
Specifies the join condition. You can reference the input mappings by their name. Note that
for using an expression, the `join` mapping requires exactly two input mappings.

* `columns` **(optional)** *(type: list:string)* *(default: empty)*:
As an alternative to explicitly specifying a join condition, you can also specify a list
of column names to join on. These columns need to be existing in all input mappings.

* `mode` **(optional)** *(type: string)* *(default: left)*:
Specifies the join mode. The following modes are supported:
`inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`, `right`, `right_outer`.

* `filter` **(optional)** *(type: string)* *(default: empty)*:
An optional SQL filter expression that is applied *after* join operation.


## Outputs
* `main` - the only output of the mapping


## Description
