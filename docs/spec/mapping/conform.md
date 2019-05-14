---
layout: page
title: Flowman Conform Mapping
permalink: /spec/mapping/conform.html
---
# Conform Mapping
The `conform` mapping performs simply name and type mangling transformations to conform data to some standard. For
example you can replace all date columns by timestamp columns (this is required for older versions of Hive) or
you can transform column names from camel case to snake case to better match SQL.

## Example
```
mappings:
  partial_facts:
    kind: conform
    input: facts
    naming: snakeCase
    types:
      date: timestamp
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `conform`

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
Specifies the name of the input mapping to be filtered.

* `naming` **(optional)** *(type: string)*:
Specifies the naming scheme used for the output. The following values are supported:
  * `camelCase` - This will 
  * `snakeCase`
  * `camelCaseUpper`

* `types` **(optional)** *(type: map:string)*:
Specifies the list of types and how they should be replaced


## Description
