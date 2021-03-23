# Hive View Relations

The `hiveView` relation is used for managing Hive tables. Although you cannot write to a Hive view, the relation can
still be useful for managing the lifecycle, i.e. for creating, migrating and destroying the Hive view. Flowman can
automatically generate the SQL from other mappings. 

## Example
```yaml
mappings:
  transaction_latest:
    kind: latest
    ...

relations:
  transaction_latest:
    kind: hiveView
    database: banking
    view: transaction_latest
    mapping: transaction_latest
```

## Fields
* `kind` **(mandatory)** *(string)*: `hiveView`

* `description` **(optional)** *(string)* *(default: empty)*:
 A description of the relation. This is purely for informational purpose.
 
* `database` **(optional)** *(string)* *(default: empty)*:
 Defines the Hive database where the view is defined. When no database is specified, the  table is accessed without
 any specific qualification, meaning that the default database will be used.

* `view` **(optional)** *(string)* *(default: empty)*:
 Contains the name of the Hive view.

* `sql` **(optional)** *(string)* *(default: empty)*:
 Contains the SQL code of the Hive view. Cannot be used together with `mapping`.

* `mapping` **(optional)** *(string)* *(default: empty)*:
 Specifies the name of a mapping, which should be translated into SQL and stored in the Hive view. Cannot be used
 together with `sql`.
 

## Description
