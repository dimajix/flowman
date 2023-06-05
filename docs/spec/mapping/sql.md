# SQL Mapping
The `sql` mapping allows executing any SQL transformation which contains Spark SQL code. You can reference other
mappings in `FROM` clauses by the mappings names. Note that you cannot reference any Hive tables, since Flowman will
interpret all tables names as references to Flowman mappings.

## Example
```yaml
mappings:
  # Mapping to read from 'people_internal' relation (not shown)
  people_internal:
    kind: relation
    relation: people_internal

  # Mapping to read from 'people_external' relation (not shown)
  people_external:
    kind: relation
    relation: people_external

  # SQL mapping which unions some information from both mappings above.
  people_union:
    kind: sql
    sql: "
      SELECT
        first_name,
        last_name
      FROM
        people_internal

      UNION ALL

      SELECT
        first_name,
        last_name
      FROM
        people_external
    "
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `sql`

* `broadcast` **(optional)** *(type: boolean)* *(default: false)*: 
Hint for broadcasting the result of this mapping for map-side joins.

* `cache` **(optional)** *(type: string)* *(default: NONE)*:
Cache mode for the results of this mapping. Supported values are
  * `NONE` - Disables caching of teh results of this mapping
  * `DISK_ONLY` - Caches the results on disk
  * `MEMORY_ONLY` - Caches the results in memory. If not enough memory is available, records will be uncached.
  * `MEMORY_ONLY_SER` - Caches the results in memory in a serialized format. If not enough memory is available, records will be uncached.
  * `MEMORY_AND_DISK` - Caches the results first in memory and then spills to disk.
  * `MEMORY_AND_DISK_SER` - Caches the results first in memory in a serialized format and then spills to disk.

* `sql` **(optional)** *(type: string)* *(default: empty)*: 
The SQL statement to execute. Inside the SQL statement you can reference other Flowman mappings. Note that you 
cannot reference Hive tables within the SQL statement, as Flowman will interpret all table names as mapping names.

* `file` **(optional)** *(type: string)* *(default: empty)*: 
The name of a file containing the SQL to execute.

* `uri` **(optional)** *(type: string)* *(default: empty)*: 
A URL pointing to a resource containing the SQL to execute.


## Outputs
* `main` - the only output of the mapping


## Description
The `sql` mapping is easy to use, you can simply type in the SQL to be executed. Flowman will
take care of instantiating all upstream mappings, which are referenced as table names in the
SQL statement.
