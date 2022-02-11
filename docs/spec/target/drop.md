# Drop Relation Target

The `drop` target is used for dropping relations, i.e. dropping tables in relational database,
dropping tables in Hive or removing output directories. The target can be used for cleaning up tables which are not
used any more.

## Example

```yaml
targets:
  drop_stations:
    kind: drop
    relation: stations

relations:
  stations:
    kind: file
    format: parquet
    location: "$basedir/stations/"
    schema:
      kind: avro
      file: "${project.basedir}/schema/stations.avsc"
```

You can also directly specify the relation inside the target definition. This saves you
from having to create a separate relation definition in the `relations` section. This is only recommended, if you
do not access the target relation otherwise, such that a shared definition would not provide any benefit.
```yaml
targets:
  drop_stations:
    kind: drop
    relation:
      kind: file
      name: stations-relation
      format: parquet
      location: "$basedir/stations/"
      schema:
        kind: avro
        file: "${project.basedir}/schema/stations.avsc"
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `drop`

* `description` **(optional)** *(type: string)*:
  Optional descriptive text of the build target

* `relation` **(mandatory)** *(type: string)*: 
Specifies the name of the relation to drop, or alternatively directly embeds the relation.


## Description

The `drop` target will drop a relation and all its contents. It will be executed both during the `CREATE` phase and
during the `DESTROY` phase.


## Supported Phases
* `CREATE` - This will drop  the target relation or migrate it to the newest schema (if possible).
* `VERIFY` - This will verify that the target relation does not exist any more
* `DESTROY` - This will also drop the relation itself and all its content.
