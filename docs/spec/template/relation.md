# Relation Template

## Example
```yaml
templates:
  key_value:
    kind: relation
    parameters:
      - name: key
        type: string
      - name: value
        type: int
        default: 12
    template:
      kind: values
      records:
        - ["$key",$value]
      schema:
        kind: embedded
        fields:
          - name: key_column
            type: string
          - name: value_column
            type: integer

relation:
  source_1:
    kind: template/key_value
    key: some_value
  source_2:
    kind: template/key_value
    key: some_other_value
    value: 13
```
