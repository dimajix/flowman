# Delta Vacuum Target

The `deltaVacuum` target is used to truncate the history stored in DeltaLake. 

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

* `relation` **(mandatory)** *(type: string or relation)*: Either the name of a `deltaTable` or `deltaFile` relation
 or alternatively an embedded delta relation

* `retentionTime` **(optional)** *(type: duration)*: Explicitly specify the retention period, i.e. how much history
 should be kept. If this field is not specified, the property `deletedFileRetentionDuration` specified in the delta 
 relation will be used instead. Note that this time is actually measured in *hours*, i.e. it doesn't make any sense
 to specify a time period with a finer granularity.


## Supported Phases
* `BUILD` - This will execute the vacuum operation
