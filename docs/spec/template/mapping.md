# Mapping Template

## Example
```yaml
# All template definitions (independent of their kind) go into the templates section
templates:
  key_value:
    # The template is a mapping template  
    kind: mapping
    # Specify a list of template parameters, which then can be provided during instantiation
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
        kind: inline
        fields:
          - name: key_column
            type: string
          - name: value_column
            type: integer

# Now you can create instances of the template in the corresponding entity section or at any other place where
# a mapping is allowed
mappings:
  # First instance  
  mapping_1:
    kind: template/key_value
    key: some_value
    
  # Second instance
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
