# Templates

Since version 0.18.0 Flowman supports a new mechanism for defining templates, i.e. reusable chunks of specifications
for relations, mappings and targets.

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
Flowman supports templates for different entity types.

```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:

   *
```
