# Documenting Mappings

As with other entities, Flowman tries to automatically infer a meaningful documentation for mappings, especially
for the schema of all mappings outputs. But of course this is not always possible especially when mappings perform
complex transformations such that a single output column depends on multiple input columns. Probably the most
complex example is the [SQL](../spec/mapping/sql.md) mapping which allows to implement most complex transformations.

In order to mitigate this issue, you can explicitly provide additional documentation for mappings via the
`documentation` tag, which is supported by all mappings.

## Example

```yaml
mappings:
  # Extract multiple columns from the raw measurements data using SQL SUBSTR functions
  measurements_extracted:
    kind: select
    input: measurements_raw
    columns:
      usaf: "SUBSTR(raw_data,5,6)"
      wban: "SUBSTR(raw_data,11,5)"
      date: "TO_DATE(SUBSTR(raw_data,16,8), 'yyyyMMdd')"
      time: "SUBSTR(raw_data,24,4)"
      air_temperature: "CAST(SUBSTR(raw_data,88,5) AS FLOAT)/10"
      air_temperature_qual: "SUBSTR(raw_data,93,1)"

    documentation:
      columns:
        - name: usaf
          description: "The USAF (US Air Force) id of the weather station"
        - name: wban
          description: "The WBAN id of the weather station"
        - name: date
          description: "The date when the measurement was made"
        - name: time
          description: "The time when the measurement was made"
        - name: report_type
          description: "The report type of the measurement"
          description: "The quality indicator of the wind speed. 1 means trustworthy quality."
        - name: air_temperature
          description: "The air temperature in degree Celsius"
        - name: air_temperature_qual
          description: "The quality indicator of the air temperature. 1 means trustworthy quality."
```

## Fields

* `description` **(optional)** *(type: string)*: A description of the mapping

* `columns` **(optional)** *(type: schema)*: A documentation of the output schema. Note that Flowman will inspect
the schema of the mapping itself and only overlay the provided documentation. Only fields found in the original
output schema will be documented, so you cannot add fields to the documentation which actually do not exist.
