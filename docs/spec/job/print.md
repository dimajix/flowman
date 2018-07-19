---
layout: page
title: Flowman Print Task
permalink: /spec/job/print.html
---
# Print Task

## Example
```
jobs:
  main:
    tasks:
      - kind: print
        description: "Print some basic project properties"
        text:
          - "project.name=${project.name}"
          - "project.version=${project.version}"
          - "project.basedir=${project.basedir}"
          - "project.filename=${project.filename}"
```

## Fields

## Description
