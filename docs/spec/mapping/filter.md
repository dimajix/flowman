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
* `input` **(mandatory)** *(type: string)*:
* `condition` **(mandatory)** *(type: string)*:


## Description
The `filter` mapping essentially corresponds to a SQL `WHERE` or `HAVING` clause.
