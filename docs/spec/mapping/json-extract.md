---
layout: page
title: Flowman Json-Extract Mapping
permalink: /spec/mapping/json-extract.html
---
# JSON Extract Mapping
In order to work with JSON data, the `json-extract` mapping extracts structured and possibly
nested records from a JSON document. The mapping will only extract the data from a single
column and will throw away any other input column.

Since Flowman cannot infer the JSON schema in advance, you need to explicitly specify the
schema of the JSONs to be extracted.

## Example
```
mappings:
  statement_item:
    kind: json-extract
    input: statement_item_raw
    column: value
    schema:
      kind: swagger
      file: "${project.basedir}/schema/statement-item.yml"
      entity: StatementItem
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `json-extract`
* `input` **(mandatory)** *(type: string)*:
Specifies the name of the input mapping to be filtered.

* `column` **(mandatory)** *(type: string)*:
Specifies the name of the column which contains the raw JSON

* `schema` **(mandatory)** *(type: schema)*:
Specifies the schema to extract. The JSON actually does not need to match this schema exactly,
the schema will mainly specify which columns should be extracted.

* `parseMode` **(optional)** *(type: string)* *(default: PERMISSIVE)*:
Specifies a mode for dealing with corrupt records during parsing.
   * PERMISSIVE : when it meets a corrupted record, puts the malformed string into a field configured by columnNameOfCorruptRecord, and sets other fields to null. To keep corrupt records, an user can set a string type field named columnNameOfCorruptRecord in an user-defined schema. If a schema does not have the field, it drops corrupt records during parsing. When inferring a schema, it implicitly adds a columnNameOfCorruptRecord field in an output schema.
   * DROPMALFORMED : ignores the whole corrupted records.
   * FAILFAST : throws an exception when it meets corrupted records.

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
