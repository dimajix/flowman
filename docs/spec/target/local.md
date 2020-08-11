# Local Target

The `local` target writes the output of a mapping into some local files.

## Example:
```yaml
targets:
  csv_export:
    kind: local
    mapping: some_mapping
    format: "csv"
    filename: "${export_file}"
    delimiter: ","
    quote: "\""
    escape: "\\"
    header: "true"
```

## Fields
 * `kind` **(mandatory)** *(string)*: `local`
 * `mapping` **(mandatory)** *(string)*:
 Specifies the name of the input mapping to be counted
 * `filename` **(mandatory)** *(string)*:
 * `encoding` **(optional)** *(string)* *(default: "UTF-8")*: 
 * `header` **(optional)** *(boolean)* *(default: true)*: 
 * `newline` **(optional)** *(string)* *(default: "\n")*: 
 * `delimiter` **(optional)** *(string)* *(default: ",")*: 
 * `quote` **(optional)** *(string)* *(default: "\"")*: 
 * `escape` **(optional)** *(string)* *(default: "\\")*: 
 * `columns` **(optional)** *(list)* *(default: [])*: 


## Supported Phases
* `BUILD`
* `VERIFY`
* `TRUNCATE`
* `DESTROY`
