# Truncate Target

The `truncate` target is used to truncate a relation or individual partitions of a relation. Truncating means that
the relation itself is not removed, but the contents are deleted (either all records or individual partitions).
Note that the `truncate` target is executed both as part of the `BUILD` and `TRUNCATE` phases, which might be surprising.


## Example
```yaml
targets:
  truncate_stations:
    kind: truncate
    relation: stations-relation
    partitions:
      year:
        start: $start_year
        end: $end_year

relations:
  stations:
    kind: file
    format: parquet
    location: "$basedir/stations/"
    schema:
      kind: avro
      file: "${project.basedir}/schema/stations.avsc"
    partitions:
      - name: year
        type: integer
        granularity: 1
```

Since Flowman 0.22.0, you can also directly specify the relation inside the target definition. This saves you
from having to create a separate relation definition in the `relations` section. This is only recommended, if you
do not access the target relation otherwise, such that a shared definition would not provide any benefit.
```yaml
targets:
  truncate_stations:
    kind: truncate
    partitions:
      year:
        start: $start_year
        end: $end_year
    relation: stations-relation
      kind: file
      format: parquet
      location: "$basedir/stations/"
      schema:
        kind: avro
        file: "${project.basedir}/schema/stations.avsc"
      partitions:
        - name: year
          type: integer
          granularity: 1
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `truncate`

* `description` **(optional)** *(type: string)*:
  Optional descriptive text of the build target

* `relation` **(mandatory)** *(type: string)*:
  Specifies the name of the relation to truncate, or alternatively directly embeds the relation.

* `partitions` **(optional)** *(type: map:partition)*:
  Specifies the partition (or multiple partitions) to truncate.


## Supported Phases
* `BUILD` - This will truncate the specified relation.
* `VERIFY` - This will verify that the relation (and any specified partition) actually contains no data.
* `TRUNCATE` - This will truncate the specified relation.
