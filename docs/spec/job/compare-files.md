---
layout: page
title: Flowman Compare Files
permalink: /spec/job/compare-files.html
---
# Compare Files

A `compare-fiels` task will compare two files or two directories of files. It will fail if the
contents of the files are different (if a directory is specified, the contents of all files
in each directory are concatenated). This task is useful for testing transformations against
expected results

## Example
```
jobs:
  test:
    tasks:
      - kind: compare-files
        expected: ${project.basedir}/test/expected_result.csv
        actual: output/actual_result.csv
```
## Fields

* `kind` **(mandatory)** *(type: string)*: `compare-files`

* `expected` **(mandatory)** *(type: string)*:
This parameter specifies the location of the file or the directory which make up the expected
result. In most caes this would be part of the project definition.

* `actual` **(mandatory)** *(type: string)*: 
This parameter specifies the location of the file or the directory which make up the actual
result.


## Description
