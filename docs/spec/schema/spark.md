# Flowman Spark Schema

The *Spark schema* refers to a schema produced by Apache Spark.

## Example
```yaml
kind: spark
file: "${project.basedir}/test/data/results/${relation}/schema.json"
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `spark`
* `file` **(optional)** *(type: string)*:
Specifies the path of a schema file.
* `url` **(optional)** *(type: string)*:
Specifies the URL of a schema.
* `spec` **(optional)** *(type: string)*:
Specifies the schema itself as an embedded string

Note that you can only use one of `file`, `url` or `spec`.
