---
layout: page
title: Flowman SQL Mapping
permalink: /spec/mapping/sql.html
---
# SQL Mapping
The `sql` mapping allows to execute any SQL transformation which contains Spark SQL code.

## Example
```
mappings:
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
The SQL statement to execute

* `file` **(optional)** *(type: string)* *(default: empty)*: 
The name of a file containing the SQL to execute.

* `uri` **(optional)** *(type: string)* *(default: empty)*: 
A url pointing to a resource containing the SQL to execute.


## Description
The `sql` mapping is easy to use, you can simply type in the SQL to be executed. Flowman will
take care of instantiating all upstream mappings, which are refernced as table names in the
SQL statement.
