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

mappings:
  mapping_1:
    kind: template/key_value
    key: some_value
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
