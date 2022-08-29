# Hive Query Relations

The `hiveQuery` relation is used for executing a SQL query against Hive tables.

## Example

### Embedded SQL
You can of course also directly specify an SQL to be executed. Note that in contrast to the [SQL mapping](../mapping/sql.md),
all table identifiers used in the SQL actually refer to Hive tables and not to Flowman mappings. The SQL will be executed
directly with Spark without providing the outputs of other mappings as tables, which means that you cannot reference any 
Flowman mappings in the SQL.
```yaml
relations:
  transaction_latest:
    kind: hiveQuery
    sql: "
      WITH tx AS (
        SELECT
          *,
          row_number() OVER(PARTITION BY transaction_id ORDER BY event_time) AS rank
        FROM sap.transaction
      )
      SELECT
        *
      FROM tx
      WHERE rank = 1
    "
```

### External SQL
You can also specify the name of an external file containing the sql by using the `file` propery instead as follows:
```yaml
relations:
  transaction_latest:
    kind: hiveQuery
    file: "${project.basedir}/sql/transaction_latest.sql"
```
And then the file `transaction_latest.sql` has to contain the query:
```sql
WITH tx AS (
  SELECT
    *,
    row_number() OVER(PARTITION BY transaction_id ORDER BY event_time) AS rank
  FROM sap.transaction
)
SELECT
  *
FROM tx
WHERE rank = 1
```

## Fields
* `kind` **(mandatory)** *(string)*: `hiveQuery`

* `schema` **(optional)** *(type: schema)* *(default: empty)*:
  Explicitly specifies the schema of the Hive source. Alternatively Flowman will automatically  try to infer the schema.

* `description` **(optional)** *(string)* *(default: empty)*:
 A description of the relation. This is purely for informational purpose.

* `sql` **(optional)** *(string)* *(default: empty)*:
 Contains the SQL code of the Hive view. Cannot be used together with `file`.

* `file` **(optional)** *(string)* *(default: empty)*:
  Contains the name of a file containing SQL code of the Hive query. Cannot be used together with `sql`.


## Automatic Migrations
Since Hive queries do not provide any physically persisted entities, no migration is supported nor required.


## Schema Conversion
The Hive query relation fully supports automatic schema conversion on read operations as described in the
corresponding section of [relations](index.md).


## Output Modes
A Hive query cannot be written to at all, therefore no output mode is supported.
