# Target Template

## Example
```yaml
templates:
  copy_table:
    kind: target
    arguments:
      - sourceTable
      - targetTable
    template:
      kind: copy
      source:
        kind: relation
        relation:
          kind: hiveTable
          database: crm
          table: ${sourceTable}_latest
      target:
        kind: relation
        relation:
          kind: jdbc
          connection: some_connection
          database: crm
          table: ${targetTable}

targets:
  copy_card:
    kind: template/copy_table
    sourceTable: card
    targetTable: card

  copy_customer:
    kind: template/copy_table
    sourceTable: customer
    targetTable: customer
```
