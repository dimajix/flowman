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
 * `description` **(optional)** *(type: string)*:
  Optional descriptive text of the build target
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


## Supported Execution Phases
* `BUILD` - build the target files containing records
* `VERIFY` - verifies that the target file exists
* `TRUNCATE` - removes the target file
* `DESTROY` - removes the target file, equivalent to `TRUNCATE`

Read more about [execution phases](../../concepts/lifecycle.md).


## Provided Metrics
The relation target also provides some metric containing the number of records written:

* Metric `target_records` with the following set of attributes
    - `name` - The name of the target
    - `category` - Always set to `target`
    - `kind` - Always set to `local`
    - `namespace` - Name of the namespace (typically `default`)
    - `project` - Name of the project
    - `version` - Version of the project

See [Execution Metrics](../../cookbook/metrics.md) for more information how to use these metrics.
