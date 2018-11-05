---
layout: page
title: Flowman Project Mapping
permalink: /spec/mapping/project.html
---
# Project Mapping
The `project` mapping performs a *projection* of an input mapping onto a specific set of columns
and also performs type conversions. This corresponds to a simple SQL `SELECT` with a series of
`CAST` expressions.

## Example
```
mappings:
  partial_facts:
    kind: project
    input: facts
    columns:
      id: String
      temperature: Float
      wind_speed: Float
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `project`

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

* `columns` **(mandatory)** *(type: map:string)*:


## Description
