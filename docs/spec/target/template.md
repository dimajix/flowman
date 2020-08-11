# Flowman Target Template

## Example

```yaml
targets:
  structured_macro:
    kind: relation
    relation: ${table}
    mode: OVERWRITE

  fee:
    kind: template
    relation: structured_macro
    environment:
    - table=fee
```
