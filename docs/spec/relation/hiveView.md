# Hive View Relations

The `hiveView` relation is used for managing Hive tables. Although you cannot write to a Hive view, the relation can
still be useful for managing the lifecycle, i.e. for creating, migrating and destroying the Hive view. Flowman can
automatically generate the SQL from other mappings. 

## Example
You can either specify a [mapping](../mapping/index.md) as the source of the Hive view. Flowman will then create an
SQL which is equivalent to the logic contained in the mapping. This only works if all (direct and indirect) sources
of the mapping also refer to Hive relations like views or tables. Also note that the process of transforming a data
flow into an SQL is non trivial, and therefore not all kinds of operation sequences might be supported.
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

As a possibly convenient alternative to create a Hive view from a mapping, you can of course also directly specify
an SQL. Note that in contrast to the [SQL mapping](../mapping/sql.md), all table identifiers used in the SQL actually
refer to Hive tables and not to Flowman mappings. The SQL will be passed as is to Hive.
```yaml
relations:
  transaction_latest:
    kind: hiveView
    database: banking
    view: transaction_latest
    sql: "
      WITH tx AS (
        SELECT
          *,
          row_number() OVER(PARTITION BY transaction_id ORDER BY event_time) AS rank
        FROM transaction
      )
      SELECT
        *
      FROM tx
      WHERE rank = 1
    "
```


## Fields
* `kind` **(mandatory)** *(string)*: `hiveView`

* `description` **(optional)** *(string)* *(default: empty)*:
 A description of the relation. This is purely for informational purpose.
 
* `database` **(optional)** *(string)* *(default: empty)*:
 Defines the Hive database where the view is defined. When no database is specified, the  table is accessed without
 any specific qualification, meaning that the default database will be used.

* `view` **(mandatory)** *(string)* *(default: empty)*:
 Contains the name of the Hive view.

* `sql` **(optional)** *(string)* *(default: empty)*:
 Contains the SQL code of the Hive view. Cannot be used together with `mapping`.

* `mapping` **(optional)** *(string)* *(default: empty)*:
 Specifies the name of a mapping, which should be translated into SQL and stored in the Hive view. Cannot be used
 together with `sql`.


## Automatic Migrations
Flowman supports automatic migration of Hive views once the view definition changes. Then Flowman will simply recreate
the Hive view with the new definition. Flowman also detects if the schema changes, which also requires a recreation
of the view to update type information stored in the Hive meta store.


## Schema Conversion
The Hive view relation fully supports automatic schema conversion on read operations as described in the
corresponding section of [relations](index.md).


## Output Modes
A Hive view cannot be written to at all, therefore no output mode is supported.
