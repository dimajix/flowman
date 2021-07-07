# JSON Schema
The *JSON schema* refers to a JSON schema definition. Note that by the nature of JSON, there is no well defined
ordering of fields within the definition.

## Plugin

This schema type is provided as part of the `flowman-json` plugin, which needs to be enabled in your 
`namespace.yml` file. See [namespace documentation](../namespace.md) for more information for configuring plugins.


## Example
```yaml
kind: json
file: "${project.basedir}/test/data/results/${relation}/schema.json"
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `json`
* `file` **(optional)** *(type: string)*:
Specifies the path of a schema file.
* `url` **(optional)** *(type: string)*:
Specifies the URL of a schema.
* `spec` **(optional)** *(type: string)*:
Specifies the schema itself as an embedded string

Note that you can only use one of `file`, `url` or `spec`.
