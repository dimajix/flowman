# Templates

Since version 0.18.0 Flowman supports a new mechanism for defining templates, i.e. reusable chunks of specifications
for relations, mappings and targets. The basic idea is that once a template is defined, it can be used in a very
similar way as built in entities (like mappings, relations and targets).

There are some differences when creating an instance of a template as opposed to normal entites:
1. Currently templates only support simple parameters like strings, integers and so on. Templates do not support
   more sophisticated data types like nested structs, maps or arrays.
2. The `kind` of a template instance always starts with `template/` for technical reasons.
3. Parameters of template instances are validated once the instance is used and not at parsing time.

## Example
```yaml
# The 'templates' section contains template definitions for relation, mappings, connections and more
templates:
  # Define a new template called "key_value" 
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
    # Now comes the template definition itself.    
    template:
      # Specify the kind within the "mapping" entitiy class  
      kind: values
      # The following settings are all specific to the "values" mapping kind
      records:
        - ["$key",$value]
      schema:
        kind: inline
        fields:
          - name: key_column
            type: string
          - name: value_column
            type: integer

# Now we can use the "key_value" template in the "mappings" section
mappings:
  # First instance  
  mapping_1:
    # You need to prefix the template name with "template/"
    kind: template/key_value
    # Provide a value for the "key" parameter. 
    # The "value" parameter has a default value, so it doesn't need to be provided
    key: some_value
    
  # Second instance
  mapping_2:
    kind: template/key_value
    key: some_other_value
    value: 13
```

## Template Types
Flowman supports templates for different entity types, namely mappings, relations and targets.

```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:

   *
```
