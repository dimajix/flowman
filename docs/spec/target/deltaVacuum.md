# Delta Vacuum Target

The `deltaVacuum` target is used to truncate the history stored in DeltaLake. 

## Plugin
The `deltaVacuum` target is provided by the [Delta Plugin](../../plugins/delta.md), which needs to be enabled in your
`namespace.yml` file. See [namespace documentation](../namespace.md) for more information for configuring plugins.

## Example

```yaml
relations:
  crm_customer:
    kind: deltaTable
    database: crm
    table: customer
    
targets:
  vacuum_crm_customer:
    kind: deltaVacuum
    relation: crm_customer
    compaction: true
    minFiles: 10
    maxFiles: 20
```

Or alternatively with an embedded relation:
```yaml
targets:
  vacuum_crm_customer:
    kind: deltaVacuum
    retentionTime: P10D
    relation:
      kind: deltaTable
      database: crm
      table: customer
```


## Fields

* `kind` **(mandatory)** *(type: string)*: `deleteFile`

* `description` **(optional)** *(type: string)*:
  Optional descriptive text of the build target

* `relation` **(mandatory)** *(type: string or relation)*: Either the name of a `deltaTable` or `deltaFile` relation
 or alternatively an embedded delta relation

* `retentionTime` **(optional)** *(type: duration)*: Explicitly specify the retention period, i.e. how much history
 should be kept. If this field is not specified, the property `deletedFileRetentionDuration` specified in the delta 
 relation will be used instead. Note that this time is actually measured in *hours*, i.e. it doesn't make any sense
 to specify a time period with a finer granularity.

* `compaction` **(optional)** *(type: boolean)* *(default: false)*: Perform compaction before vacuum operation. A
 compaction will reduce the number of files per table and/or partition

* `minFiles` **(optional)** *(type: integer)* *(default: 16)*: Number of files per partition, which will be created
 during a compaction operation.

* `maxFiles` **(optional)** *(type: integer)* *(default: 64)*: Number of files per partition, below which no compaction
 will be performed.


## Supported Execution Phases
* `BUILD` - This will execute the vacuum operation

Read more about [execution phases](../../concepts/lifecycle.md).
