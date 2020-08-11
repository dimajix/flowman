# Flowman Copy Target

The copy target can be used to copy contents of one data set to another. A dataset can be 'file', 'mapping', 'relation'
or other supported types.

## Example

```yaml
targets:
  stations:
    kind: copy
    source:
      kind: relation
      relation: weather_records
      partition:
        processing_date: "${processing_date}"
    target:
      kind: file
      format: csv
      location: "/landing/weather/data"
    schema:
      format: spark
      file: "/landing/weather/schema.json"
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `copy`

* `source` **(mandatory)** *(type: dataset)*: 
Specifies the source data set to be copied

* `target` **(mandatory)** *(type: dataset)*: 
Specifies the target data set to be copied

* `schema` **(optional)**:
Optionally specify a schema to be written.


## Supported Phases
* `BUILD` - The *build* phase will perform the copy operation
* `VERIFY` - The *verify* phase will ensure that the target exists
* `TRUNCATE` - The *truncate* phase will remove the target
* `DESTROY` - The *destroy* phase will remove the target
