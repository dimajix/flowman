# JSON Unpack Mapping

## Example
```yaml
mappings:
  m0:
    kind: unpackJson
    input: source
    columns:
      - name: json_column
        schema:
          kind: embedded
          fields:
            - name: name
              type: String
            - name: age
              type: Integer
            - name: details
              type:
                kind: struct
                fields:
                  - name: firstName
                    type: string
                  - name: lastName
                    type: string
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `unpackJson`

* `broadcast` **(optional)** *(type: boolean)* *(default: false)*: 
Hint for broadcasting the result of this mapping for map-side joins.

* `cache` **(optional)** *(type: string)* *(default: NONE)*:
Cache mode for the results of this mapping. Supported values are
  * `NONE` - Disables caching of teh results of this mapping
  * `DISK_ONLY` - Caches the results on disk
  * `MEMORY_ONLY` - Caches the results in memory. If not enough memory is available, records will be uncached.
  * `MEMORY_ONLY_SER` - Caches the results in memory in a serialized format. If not enough memory is available, records will be uncached.
  * `MEMORY_AND_DISK` - Caches the results first in memory and then spills to disk.
  * `MEMORY_AND_DISK_SER` - Caches the results first in memory in a serialized format and then spills to disk.

* `input` **(mandatory)** *(type: string)*:
Specifies the name of the input mapping to be filtered.

* `parseMode` **(optional)** *(type: string)* *(default: PERMISSIVE)*:
Specifies a mode for dealing with corrupt records during parsing.
   * `PERMISSIVE` : when it meets a corrupted record, puts the malformed string into a field configured by columnNameOfCorruptRecord, and sets other fields to null. To keep corrupt records, an user can set a string type field named columnNameOfCorruptRecord in an user-defined schema. If a schema does not have the field, it drops corrupt records during parsing. When inferring a schema, it implicitly adds a columnNameOfCorruptRecord field in an output schema.
   * `DROPMALFORMED` : ignores the whole corrupted records.
   * `FAILFAST` : throws an exception when it meets corrupted records.

* `corruptedColumn` **(optional)** *(type: string)* *(default: _corrupt_record)*:
* `allowComments` **(optional)** *(type: boolean)* *(default: false)*:
If set to true, the mapping ignores Java/C++ style comment in JSON records

* `allowUnquotedFieldNames` **(optional)** *(type: boolean)* *(default: false)*:
If set to true, the mapping allows unquoted JSON field names

* `allowSingleQuotes` **(optional)** *(type: boolean)* *(default: true)*:
Allows single quotes in addition to double quotes

* `allowNumericLeadingZeros` **(optional)** *(type: boolean)* *(default: false)*:
Allows leading zeros in numbers (e.g. 00012)

* `allowNonNumericNumbers` **(optional)** *(type: boolean)* *(default: true)*:

* `allowBackslashEscapingAnyCharacter` **(optional)** *(type: boolean)* *(default: false)*:
Allows accepting quoting of all character using backslash quoting mechanism
 
* `allowUnquotedControlChars` **(optional)** *(type: boolean)* *(default: false)*:
Allows JSON Strings to contain unquoted control characters (ASCII characters with value less
than 32, including tab and line feed characters) or not.


## Description
