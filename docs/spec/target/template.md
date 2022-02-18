# Template Target

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

## Supported Execution Phases

The supported execution phases are determined by the referenced target.
