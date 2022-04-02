# Values Dataset

## Example

```yaml
kind: values  
schema:
  kind: inline
  fields:
    - name: int_col
      type: integer
    - name: str_col
      type: string
records:
    - [1,"some_string"]
    - [2,"cat"]
```

```yaml
kind: values
columns:
  int_col: integer
  str_col: string
records:
    - [1,"some_string"]
    - [2,"cat"]
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `values` or `const`

* `records` **(optional)** *(type: list:array)* *(default: empty)*:
  An optional list of records to be returned.

* `columns` **(optional)** *(type: map:string)*:
  Specifies the list of column names (key) with their type (value)

* `schema` **(optional)** *(type: schema)*:
  As an alternative of specifying a list of columns you can also directly specify a schema.
