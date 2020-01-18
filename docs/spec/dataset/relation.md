# Flowman Relation Dataset

The *relation dataset* refers to any named relation defined in Flowman. By specifying partition values, the data set
can refer to a specific partition of the relation, otherwise it will refer to the full relation. This
feature can be interesting for reading all partitions from a partitioned relation.


## Example
```yaml
kind: relation
relation: ${relation}
partition:
  landing_date: ${landing_date}
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `relation`
* `relation` **(mandatory)** *(type: string)*: Name of the relation
* `partition` **(optional)** *(type: map)*: Map of partition names and their values
