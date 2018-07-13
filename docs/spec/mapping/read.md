# Read Mapping


## Example
```
mappings:
  measurements-raw:
    kind: read-relation
    source: measurements-raw
    partitions:
      year:
        start: $start_year
        end: $end_year
    columns:
      raw_data: String
```

## Fields

* `kind` **(mandatory)** *(string)*: `read` or `read-relation`
* `source` **(mandatory)** *(string)*:
* `partitions` **(optional)** *(map:partition)*:
* `columns` **(optional)** *(map:data_type)*:


## Description
