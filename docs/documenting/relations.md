# Documenting Relations

As with other entities, Flowman tries to automatically infer a meaningful documentation for mappings, especially
for the schema of a relation. In order to do so, Flowman will query the original data source and look up any
metadata (for example Flowman will pick up column descriptions in the Hive Metastore).

In order to provide additiona information, you can explicitly provide additional documentation for mappings via the
`documentation` tag, which is supported by all mappings.

## Example

```yaml
relations:
  aggregates:
    kind: file
    format: parquet
    location: "$basedir/aggregates/"
    partitions:
      - name: year
        type: integer
        granularity: 1
        
    # Explicit documentation section for annotating columns of the relation
    documentation:
      description: "The table contains all aggregated measurements"  
      columns:
        # You can document any column you like, you don't have to provide a description for all of them
        - name: country
          description: "Country of the weather station"
        - name: min_temperature
          description: "Minimum air temperature per year in degrees Celsius"
        - name: max_temperature
          description: "Maximum air temperature per year in degrees Celsius"
        - name: avg_temperature
          description: "Average air temperature per year in degrees Celsius"
```

## Fields

* `description` **(optional)** *(type: string)*: A description of the mapping

* `columns` **(optional)** *(type: schema)*: A documentation of the output schema. Note that Flowman will inspect
  the schema of the mapping itself and only overlay the provided documentation. Only fields found in the original
  output schema will be documented, so you cannot add fields to the documentation which actually do not exist.
