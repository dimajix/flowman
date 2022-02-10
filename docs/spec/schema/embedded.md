# Embedded Schema

The embedded schema is (as the name already suggests) directly embedded into the corresponding yml file.

## Example

```yaml
relations:
  input:
    kind: csv
    location: "${logdir}"
    options:
      delimiter: "\t"
      quote: "\""
      escape: "\\"
    schema:
      kind: embedded
      fields:
        - name: UnixDateTime
          type: Long
        - name: Impression_Uuid
          type: String
        - name: Event_Type
          type: Integer
        - name: User_Uuid
          type: String
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `embedded`

* `fields` **(mandatory)** *(type: list:field)*: Contains all fields


## Field properties

* `name` **(mandatory)** *(type: string)*: specifies the name of the column
* `type` **(mandatory)** *(type: data type)*: specifies the data type of the column
* `nullable` **(optional)** *(type: boolean)* *(default: true)*
* `description` **(optional)** *(type: string)*
* `default` **(optional)** *(type: string)* Specifies a default value
* `format` **(optional)** *(type: string)* Some relations or file formats may support different formats for example
for storing dates


## Data Types

The following simple data types are supported by Flowman

* `string`, `text` - text and strings of arbitrary length
* `binary` - binary data of arbitrary length
* `tinyint`, `byte` - 8 bit signed numbers
* `smallint`, `short` - 16 bit signed numbers
* `int`, `integer` - 32 bit signed numbers
* `bigint`, `long` - 64 bit signed numbers
* `boolean` - true or false
* `float` - 32 bit floating point number
* `double` - 64 bit floating point number
* `decimal(a,b)`
* `varchar(n)` - text with up to `n`characters. Note that this data type is only supported for specifying input or
output data types. Internally Spark and therefore Flowman convert these columns to a `string` column of arbitrary length.
* `char(n)` - text with exactly `n`characters. Note that this data type is only supported for specifying input or
  output data types. Internally Spark and therefore Flowman convert these columns to a `string` column of arbitrary length.
* `date` - date type
* `timestamp` - timestamp type (date and time)
* `duration` - duration type

In addition to those simple data types the following complex types are supported:

* `struct` for creating nested data types
```yaml
name: some_struct
type:
  kind: struct
  fields:
    - name: some_field
      type: int
    - name: some_other_field
      type: string
```

* `map`
```yaml
name: keyValue
type:
  kind: map
  keyType: string
  valueType: int
```

* `array` for storing arrays of sub elements
 ```yaml
name: names
type:
  kind: array
  elementType: string
```
