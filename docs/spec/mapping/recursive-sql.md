
# Recursive SQL Mapping
The `recursiveSql` mapping allows to execute recursive SQL transformation which contains Spark SQL code.

## Example
```
mappings:
  factorial:
    kind: recursiveSql
    sql: "
        SELECT
             0 AS n,
             1 AS fact
        
        UNION ALL
        
        SELECT
             n+1 AS n,
             (n+1)*fact AS fact
        FROM __this__
        WHERE n < 6
    "
```

## Fields
* `kind` **(mandatory)** *(type: string)*: `recursiveSql`

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
The SQL statement to execute

* `file` **(optional)** *(type: string)* *(default: empty)*: 
The name of a file containing the SQL to execute.

* `uri` **(optional)** *(type: string)* *(default: empty)*: 
A url pointing to a resource containing the SQL to execute.


## Outputs
* `main` - the only output of the mapping


## Description
The `recursiveSql` mapping allows to execute recursive SQL statements, which refer to themselves. The result of each
step is made available as a temporary table `__this__`. Currently the query has to be a `UNION` where the first part
may not contain a reference to `__this__`. The first part of the `UNION` will be used to determine the schema of the
result.
