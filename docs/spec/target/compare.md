# Compare Target

The *compare target* performs a comparison of all records of two [datasets](../dataset/index.md) in the verification
phase. If some records do not match or are missing, the processing will stop. This way the compare target can be
used for implementing tests.

## Example
```yaml
targets:
  verify_result_macro:
    kind: compare
    actual:
      kind: relation
      relation: ${relation}
    expected:
      kind: file
      format: json
      location: "${project.basedir}/test/data/results/${relation}/data.json"
      schema:
        kind: spark
        file: "${project.basedir}/test/data/results/${relation}/schema.json"
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `relation`

* `description` **(optional)** *(type: string)*:
  Optional descriptive text of the build target

* `actual` **(mandatory)** *(type: dataset)*: 
Specifies the data set containing the actual data. Often you will either use a relation written to by Flowman or
a mapping. 

* `expected` **(mandatory)** *(type: dataset)*: 
Specifies the data set containing the expected data. In most cases you probably will use a file data set referencing
some predefined results

## Supported Execution Phases
* `VERIFY` - Comparison will be performed in the *verify* build phase. If the comparison fails, the build will stop
with an error

Read more about [execution phases](../../concepts/lifecycle.md).
