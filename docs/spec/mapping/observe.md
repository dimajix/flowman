# Observe Mapping
Since Flowman 0.30.0, requires Spark 3.0 or later.

The `observe` mapping is used to collect data dependent metric information from within the whole data flow. The
execution is relatively cheap, although the mapping actually is an optimization barrier for Spark.

Typical use cases are counting records, examining the min and max value of a column.


## Example
```yaml
mappings:
  measurements:
    kind: relation
    relation: measurements
    
  inspect_measurements:
    kind: observe
    input: measurements
    measures:
      record_count: "count(*)"
      min_temperature: "min(air_temperature)"
      max_temperature: "max(air_temperature)"
```


## Fields
* `kind` **(mandatory)** *(type: string)*: `observe`

* `broadcast` **(optional)** *(type: boolean)* *(default: false)*:
  Hint for broadcasting the result of this mapping for map-side joins.

* `cache` **(optional)** *(type: string)* *(default: NONE)*:
  Cache mode for the results of this mapping. Supported values are
  * `NONE`
  * `DISK_ONLY`
  * `MEMORY_ONLY`
  * `MEMORY_ONLY_SER`
  * `MEMORY_AND_DISK`
  * `MEMORY_AND_DISK_SER`

* `input` **(optional)** *(type: string)*:
  Specifies the name of the input mapping, which should be observed with the measures defined in the `measures` 
  section.

* `measures` **(optional)** *(type: map:array)* *(default: empty)*:
This property defines the set of measures to be taken from the input mapping. The keys of the map will be used as
the names of corresponding metrics, while the values need to be valid SQL aggregate expressions. The measurements
must either contain a literal (e.g. `42`), or should contain one or more aggregate functions (e.g. `sum(a)` or 
`sum(a + b) + avg(c) - 1`). Expressions that  contain references to the input mappings columns must always be wrapped 
in an aggregate function.


## Outputs
The `observe` mapping creates one `main` output, which matches the input.


## Provided Metrics
All measures defined are exported as metrics with the same name and with the following labels:
- `name` - The name of the mapping (i.e. `inspect_measurements` above)
- `category` - Always set to `mapping`
- `kind` - Always set to `observe`
- `namespace` - Name of the namespace (typically `default`)
- `project` - Name of the project
- `version` - Version of the project
