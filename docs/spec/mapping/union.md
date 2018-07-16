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
 * `inputs` **(mandatory)** *(type: list:string)*:
 * `columns` **(optional)** *(type: map:string)* *(default: empty)*:
 * `distinct` **(optional)** *(type: boolean)* *(default: false)*:
 If set to true, only distinct records will be returned (using the specified or inferred set 
 of columns).


## Description

Essentially the `union` mapping performs a SQL `UNION ALL`. In contrast to most SQL 
implementations, the `union` mapping actually uses column names instead of column positions
for matching multiple input mappings. 

Optionally you can also set `distinct` to true, then the operation corresponds to a SQL
`UNION DISTINCT`
