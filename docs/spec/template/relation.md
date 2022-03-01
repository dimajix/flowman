# Relation Template

## Example
```yaml
# All template definitions (independent of their kind) go into the templates section
templates:
  key_value:
    # The template is a relation template  
    kind: relation
    # Specify a list of template parameters, which then can be provided during instantiation
    parameters:
      - name: key
        type: string
      - name: value
        type: int
        default: 12
    template:
      # This is now a normal relation definition, which can also access the parameters as variables  
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

# Now you can create instances of the template in the corresponding entity section or at any other place where
# a relation is allowed
relation:
  # First instance  
  source_1:
    kind: template/key_value
    key: some_value

  # Second instance
  source_2:
    kind: template/key_value
    key: some_other_value
    value: 13
```

Once a relation template is defined, you can create instances of the template at any place where a relation can be 
specified. You need to use the special syntax `template/<template_name>` when creating an instance of the template.
The template instance then can also contain values for all parameters defined in the template.


## Fields

* `kind` **(mandatory)** *(type: string)*: `relation`
* `parameters` **(optional)** *(type: list[parameter])*: list of parameter definitions.
* `template` **(mandatory)** *(type: relation)*: The actual definition of a relation.
