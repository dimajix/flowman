---
layout: page
title: Flowman Output Task
permalink: /spec/job/output.html
---
# Output Task

## Example
```
jobs:
  write-all:
    description: "Writes all outputs"
    tasks:
      - kind: output
        description: Writes all station outputs to files
        outputs:
          - stations-local
          - stations-relation-file
          - stations-relation-local
```
## Fields

* `kind` **(mandatory)** *(type: string)*: `output`
* `description` **(optional)** *(type: string)*:
* `outputs` **(mandatory)** *(type: list:string)*:

## Description
