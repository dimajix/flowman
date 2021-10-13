# Mapping Template

## Example
```yaml
# All template definitions (independent of their kind) go into the templates section
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
      # This is now a normal mapping definition, which can also access the parameters as variables  
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
# a mapping is allowed
mappings:
  mapping_1:
    kind: template/key_value
    key: some_value
  mapping_2:
    kind: template/key_value
    key: some_other_value
    value: 13
```

Once a mapping template is defined, you can create instances of the template at any place where a mapping can be
specified. You need to use the special syntax `template/<template_name>` when creating an instance of the template.
The template instance then can also contain values for all parameters defined in the template.


## Fields

* `kind` **(mandatory)** *(type: string)*: `mapping`
* `parameters` **(optional)** *(type: list[parameter])*: list of parameter definitions.
* `template` **(mandatory)** *(type: mapping)*: The actual definition of a mapping.
