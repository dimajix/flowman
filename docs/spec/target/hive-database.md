# FLowman Hive Database Target

The *Hive database target* is used for managing a Hive database. In many cases, an empty Hive database will be provided
by the operations team to you, then this target is not needed. If this is not the case, you can also manage the
lifecycle of a whole Hive database using this target.

## Example
```yaml
targets:
  database:
    kind: hiveDatabase
    database: "my_database"
```

## Supported Phases
* `CREATE` - Ensures that the specified Hive database exists and creates one if it is not found
* `VERIFY` - Verifies that the specified Hive database exists
* `DESTROY` - Drops the Hive database
