# Truncate Target

The `truncate` target is used to truncate a relation or individual partitions of a relation. Truncating means that
the relation itself is not removed, but the contents are deleted (either all records or individual partitions).
Note that the `truncate` target is executed as part of the `BUILD`phase, which might be surprising.


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
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `truncate`

* `relation` **(mandatory)** *(type: string)*:
  Specifies the name of the relation to truncate.

* `partitions` **(optional)** *(type: map:partition)*:
  Specifies the partition (or multiple partitions) to truncate.


## Supported Phases
* `BUILD` - This will truncate the specified relation.
* `VERIFY` - This will verify that the relation (and any specified partition) actually contains no data.
