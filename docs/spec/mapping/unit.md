# Unit Mapping

A `unit` mapping encapsulates a whole block of multiple mappings with a local name resolution scope. This helps to
prevent having multiple mappings with the same name at a global scope. Moreover, the `unit` mapping is often very
useful in combination with the [Template Mapping](template.md) to create complex macro-like chains of mappings.

## Example
```yaml
mappings:
  events:
    kind: unit
    mappings:
      raw:
        kind: read
        relation: "${relation}"
        partitions:
          processing_date: "${processing_date}"

      extracted:
        kind: extractJson
        input: raw
        column: value
        schema:
          kind: spark
          file: "${project.basedir}/schema/${schema}"

      error:
        kind: extend
        input: extracted:error
        columns:
          processing_date: "'${processing_date}'"
          run_date: "'${run_date}'"
          app_name: "'${project.name}'"
          app_version: "'${project.version}'"

      main:
        kind: deduplicate
        input: extracted
        columns: metadata.eventId
```
