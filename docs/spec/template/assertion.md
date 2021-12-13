# Assertion Template

## Example
```yaml
templates:
  user:
    kind: assertion
    parameters:
      - name: table
        type: string
      - name: p1
        type: int
        default: 12
    template:
      kind: sql
      query: "SELECT * FROM $table WHERE idx = $p1"

targets:
  validate_1:
    kind: validate
    assertions:
      a1:
        kind: template/user
        table: some_table
        p1: 13
  verify_1:
    kind: verify
    assertions:
      a1:
        kind: template/user
        table: some_other_value
        p1: 27
```
