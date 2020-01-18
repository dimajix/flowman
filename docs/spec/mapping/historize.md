# Historize Mapping

## Example
```yaml
mappings:
  card_history:
    kind: historize
    input: card
    keyColumns: id
    timeColumn: ts
    validFromColumn: valid_from
    validToColumn: valid_until
    columnInsertPosition: beginning
```

## Outputs
* `main` - the only output of the mapping

