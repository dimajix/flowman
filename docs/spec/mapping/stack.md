# Stack Mapping

Stacking is the operation of transposing specific columns into rows. Let's take for example the following table:

| product | EN Text | German Text | fr  |
|---------|---------|-------------|-----|
| 0001    | Shoes   | Shuhe       | ... |
| 0002    | T-Shirt | T-Shirt     | ... |
| 0003    | ....    |             |     |

A *stacking* operation using the stack columns `EN Text`, `German Text` and `fr` will result in the following new table
provided the `land` as the name of the new `nameColumn` and `text` as the name of the new value column:

| product | lang | text    |
|---------|------|---------|
| 0001    | en   | Shoes   |
| 0001    | de   | Shuhe   |
| 0001    | fr   | ...     |
| 0002    | en   | T-Shirt |
| 0002    | de   | T-Shirt |
| 0002    | fr   | ...     |
| ...     | ...  | ...     | 


## Example
```yaml
mappings:
  stack_translations:
    kind: stack
    input: translations
    filter: en <> de
    dropNulls: true
    keepColumns:
      - product
      - sub_product
    dropColumns:
      - product_description
      - sub_product_code
    nameColumn: lang
    valueColumn: text
    stackColumns:
      EN Text: en
      German Text: de
      fr: fr
```


## Fields
* `kind` **(mandatory)** *(string)*: `select`

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

* `input` **(mandatory)** *(type: string)*:

* `dropNulls` **(optional)** *(type: boolean)* *(default: false)*:
Drop all rows after stacking with a `NULL` value.

* `nameColumn` **(mandatory)** *(type: string)*:
The name of the new column to be appended which contains the mapped name of the original stacked column.

* `valueColumn` **(mandatory)** *(type: string)*:
  The name of the new column to be appended which contains the value of the original stacked column.

* `stackColumns` **(mandatory)** *(type: map)*:
 The list of columns to be stacked together with the value to be written to the `nameColumn`

* `filter` **(optional)** *(type: string)* *(default: empty)*:
  An optional SQL filter expression that is applied *after* the sort operation.


## Outputs
* `main` - the only output of the mapping
