# Target Template

## Example
```yaml
# All template definitions (independent of their kind) go into the templates section
templates:
  copy_table:
    kind: target
    parameters:
      - sourceTable
      - targetTable
    template:
      # This is now a normal target definition, which can also access the parameters as variables  
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

# Now you can create instances of the template in the corresponding entity section or at any other place where
# a target is allowed
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

Once a target template is defined, you can create instances of the template at any place where a target can be
specified. You need to use the special syntax `template/<template_name>` when creating an instance of the template.
The template instance then can also contain values for all parameters defined in the template.


## Fields

* `kind` **(mandatory)** *(type: string)*: `target`
* `parameters` **(optional)** *(type: list[parameter])*: list of parameter definitions.
* `template` **(mandatory)** *(type: target)*: The actual definition of a target
