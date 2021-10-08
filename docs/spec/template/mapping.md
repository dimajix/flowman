# Mapping Template

## Example
```yaml
templates:
  key_value:
    kind: mapping
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

mappings:
  mapping_1:
    kind: template/key_value
    key: some_value
  mapping_2:
    kind: template/key_value
    key: some_other_value
    value: 13
```
