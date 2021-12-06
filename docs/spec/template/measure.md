# Measure Template

## Example

```yaml
templates:
  user:
    kind: measure
    parameters:
      - name: $table
        type: string
      - name: p1
        type: int
        default: 12
    template:
      kind: sql
      query: "SELECT * FROM $table WHERE x = $p1"

targets:
  measure_1:
    kind: measure
    measures:
      m1:
        kind: template/user
        table: some_table
        
  measure_2:
    kind: measure
    measures:
      m1:
        kind: template/user
        table: some_other_table
        p1: 27
```
