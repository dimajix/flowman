---
layout: page
title: Flowman Filter Mapping
permalink: /spec/mapping/filter.html
---
# Filter Mapping

## Example
```
mappings:
  facts_special:
    kind: filter
    input: facts_all
    condition: "special_flag = TRUE"
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `filter`

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

* `input` **(mandatory)** *(type: string)*:
Specifies the name of the input mapping to be filtered.

* `condition` **(mandatory)** *(type: string)*:
Specifies the condition as a SQL expression to filter on


## Description
The `filter` mapping essentially corresponds to a SQL `WHERE` or `HAVING` clause. The example
above would be equivalent to the following SQL statement:
```
SELECT
    *
FROM facts_all
WHERE special_flag=TRUE
```
