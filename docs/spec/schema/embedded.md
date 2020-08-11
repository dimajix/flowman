# Embedded Schema

The embedded schema is (as the name already suggests) directly embedded into the corresponding yml file.

## Example

```yaml
relations:
  input:
    kind: csv
    location: "${logdir}"
    options:
      delimiter: "\t"
      quote: "\""
      escape: "\\"
    schema:
      kind: embedded
      fields:
        - name: UnixDateTime
          type: Long
        - name: Impression_Uuid
          type: String
        - name: Event_Type
          type: Integer
        - name: User_Uuid
          type: String
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `embedded`
* `fields` **(mandatory)** *(type: list:field)*: Contains all fields
