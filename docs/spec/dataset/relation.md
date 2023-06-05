# Relation Dataset

The *relation dataset* refers to any named relation defined in Flowman. By specifying partition values, the data set
can refer to a specific partition of the relation, otherwise it will refer to the full relation. This
feature can be interesting for reading all partitions from a partitioned relation.


## Example
```yaml
kind: relation
relation: sales
partition:
  landing_date: ${landing_date}
```

Since Flowman 0.18.0, you can also directly specify the relation inside the dataset definition. This saves you
from having to create a separate relation definition in the `relations` section.  This is only recommended, if you
do not access the target relation otherwise, such that a shared definition would not provide any benefit.
```yaml
kind: relation
relation:
  kind: hiveTable
  database: sales
  table: invoices
partition:
  landing_date: ${landing_date}
```


## Fields

* `kind` **(mandatory)** *(type: string)*: `relation`
* `relation` **(mandatory)** *(type: string or relation)*: Name of the relation, or relation definition
* `partition` **(optional)** *(type: map)*: Map of partition names and their values
