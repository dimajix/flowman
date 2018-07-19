---
layout: page
title: Flowman Union Mapping
permalink: /spec/mapping/sql.html
---
# Union Mapping

## Example

```
mappings:
  facts_all:
    kind: union
    inputs:
      - facts_rtb
      - facts_direct
```


## Fields
* `kind` **(mandatory)** *(type: string)*: `union`

* `broadcast` **(optional)** *(type: boolean)* *(default: false)*: 
Hint for broadcasting the result of this mapping for map-side joins.

* `cache` **(optional)** *(type: string)* *(default: NONE)*:
Cache mode for the results of this mapping. Supported values are
  * NONE
  * DISK_ONLY
  * MEMORY_ONLY
  * MEMORY_ONLY_SER
  * MEMORY_AND_DISK
  * MEMORY_AND_DISK_SER

* `inputs` **(mandatory)** *(type: list:string)*:
List of input mappings to build the union of.

* `columns` **(optional)** *(type: map:string)* *(default: empty)*:
Optionally you can specify the list a list of columns. Then the union will only
contain these columns, otherwise the superset of all columns of all input mappings
will be used.

* `distinct` **(optional)** *(type: boolean)* *(default: false)*:
 If set to true, only distinct records will be returned (using the specified or inferred set 
 of columns).


## Description

Essentially the `union` mapping performs a SQL `UNION ALL`. In contrast to most SQL 
implementations, the `union` mapping actually uses column names instead of column positions
for matching multiple input mappings. 

Optionally you can also set `distinct` to true, then the operation corresponds to a SQL
`UNION DISTINCT`
