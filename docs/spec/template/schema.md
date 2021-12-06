# Schema Template

## Example
```yaml
# All template definitions (independent of their kind) go into the templates section
templates:
  default_schema:
    kind: schema
    parameters:
      - name: schemaName
        type: string
    template:
      kind: avro
      file: "${project.basedir}/schema/${schemaName}.avsc"

relations:
  measurements:
    kind: file
    format: parquet
    location: "$basedir/measurements/"
    pattern: "${year}"
    partitions:
      - name: year
        type: integer
        granularity: 1
    schema:
      kind: template/default_schema
      schemaName: measurements
```
