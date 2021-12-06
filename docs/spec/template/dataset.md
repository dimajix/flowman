# Dataset Template

## Example

```yaml
templates:
  user:
    kind: dataset
    parameters:
      - name: p0
        type: string
      - name: p1
        type: int
        default: 12
    template:
      kind: values
      records:
        - ["$p0",$p1]
      schema:
        kind: embedded
        fields:
          - name: str_col
            type: string
          - name: int_col
            type: integer

targets:
  dump_1:
    kind: console
    input:
      kind: template/user
      p1: 13
  dump_2:
    kind: console
    input:
      kind: template/user
      p0: some_value
      p1: 27
```
