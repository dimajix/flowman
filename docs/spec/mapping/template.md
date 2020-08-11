# Flowman Template Mapping

A `template` mapping allows to (re-)instantiate another mapping with some environment variables set to different values.
This allows to create macro-like mappings, which then can be instantiated via the `template` mapping.

## Example
```yaml
mappings:
  events_macro:
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

  specific_events:
    kind: template
    mapping: events_macro
    environment:
    - relation=some_relation
    - schema=SpecifcEventSchema.json
```
