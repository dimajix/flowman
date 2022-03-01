# Generic Relation

A `generic` relation gives you access to Spark data relations otherwise not directly
supported by Flowman.

## Example

```yaml
relations:
  advertiser_setting:
    kind: generic
    format: "csv"
    schema:
      kind: embedded
      fields:
        - name: id
          type: Integer
        - name: advertiser_setting_id
          type: Integer
```

## Fields
* `kind` **(mandatory)** *(string)*: `generic`

* `schema` **(optional)** *(schema)* *(default: empty)*:
  Explicitly specifies the schema of the relation.

* `description` **(optional)** *(string)* *(default: empty)*:
  A description of the relation. This is purely for informational purpose.

* `options` **(optional)** *(map:string)* *(default: empty)*:
  All options are passed directly to the reader/writer backend and are specific to each
  supported format.

* `format` **(optional)** *(string)* *(default: empty)*:
  Specifies the name of the Spark data source format to use. 


## Automatic Migrations
The `generic` relation does not support any automatic migration like adding/removing columns. 
