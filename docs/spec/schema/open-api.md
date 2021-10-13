# OpenAPI Schema

The *OpenAPI schema* refers to a schema in the OpenAPI 3.0 format.

## Plugin

This schema type is provided as part of the [`flowman-openapi` plugin](../../plugins/openapi.md), which needs to be enabled in your
`namespace.yml` file. See [namespace documentation](../namespace.md) for more information for configuring plugins.


## Example
```yaml
kind: openApi
file: "${project.basedir}/test/data/results/${relation}/schema.json"
entity: CardEvent
nullable: true
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `openApi`
* `entity` **(mandatory)** *(type: string)*:
Specifies the entity in the Swagger schema to be used
* `file` **(optional)** *(type: string)*:
Specifies the path of a schema file.
* `url` **(optional)** *(type: string)*:
Specifies the URL of a schema.
* `spec` **(optional)** *(type: string)*:
Specifies the schema itself as an embedded string
* `nullable` **(optional)** *(type: boolean)* *(default: false)*:
If set to true, then all fields will be marked as *nullable* even if the Swagger definition marks them as *required*

Note that you can only use one of `file`, `url` or `spec`.
