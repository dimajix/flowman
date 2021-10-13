# Values Relation

A `values` relation contains directly specified constant values. It is a good candidate to be used for mocking data in
tests.


## Example

```yaml
relations:
  fake_input:
    kind: values  
    schema:
      kind: embedded
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
relations:
  fake_input:
    kind: values
    columns:
      int_col: integer
      str_col: string
    records:
      - [1,"some_string"]
      - [2,"cat"]
```

```yaml
relations:
  fake_input:
    kind: values
    columns:
      int_col: integer
      str_col: string
    records:
      - int_col: 1
        str_col: "some_string"
      - str_col: "cat"
```


## Fields
* `kind` **(mandatory)** *(type: string)*: `values` or `const`

* `records` **(optional)** *(type: list:array)* *(default: empty)*:
  An optional list of records to be returned.

* `columns` **(optional)** *(type: map:string)*:
  Specifies the list of column names (key) with their type (value)

* `schema` **(optional)** *(type: schema)*:
  As an alternative of specifying a list of columns you can also directly specify a schema.


## Output Modes
A `values` relation cannot be written to at all, therefore no output mode is supported.
