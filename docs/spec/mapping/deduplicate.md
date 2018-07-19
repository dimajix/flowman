---
layout: page
title: Flowman Deduplicate Mapping
permalink: /spec/mapping/deduplicate.html
---
# Deduplicate Mapping

## Example
```
mappings:
  persons_unique:
    kind: deduplicate
    input: persons
    columns:
      - first_name
      - last_name
      - birthdate
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `deduplicate`
* `input` **(mandatory)** *(type: string)*:
Specifies the name of the input mapping

* `columns` **(optional)** *(type: string)* *(default: empty)*:
Specifies the names of the columns to use for deduplication. The result will still have the
full set of columns.

## Description
