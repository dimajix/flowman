# Avro Schema
The *Avro schema* refers to a schema conforming to the Avro standard

## Example
```yaml
kind: avro
file: "${project.basedir}/test/data/results/${relation}/schema.json"
nullable: true
```

```yaml
kind: avro
spec: |
    {
    "type": "record",
    "namespace": "",
    "name": "test_schema",
    "doc": "Some Documentation",
    "fields": [
        {
            "doc": "AccessDateTime as a string",
            "type": "string",
            "name": "AccessDateTime",
            "order": "ignore"
        }
    ]
    }
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `avro`
* `file` **(optional)** *(type: string)*:
  Specifies the path of a schema file.
* `url` **(optional)** *(type: string)*:
  Specifies the URL of a schema.
* `spec` **(optional)** *(type: string)*:
  Specifies the schema itself as an embedded string
* `nullable` **(optional)** *(type: boolean)* *(default: false)*:
  If set to true, all fields will be made *nullable*.

Note that you can only use one of `file`, `url` or `spec`.
