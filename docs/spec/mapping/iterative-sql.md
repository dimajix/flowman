# Iterative SQL Mapping
The `iterativeSql` mapping allows to iteratively execute SQL transformation which contains Spark SQL code. The
iteration will stop when the data does not change anymore.

## Example
The following example will detect trees within a company hierarchy table, which provides simple parent-child
relations. The objective of the query is to assign a separate ID to each company tree. The query will essentially
propagate the `tree_id` from each parent down to its direct children. This step is performed over and over again
until the `tree_id` from the root companies without a parent are propagated to the leave companies without any
children.
```
mappings:
  organization_hierarchy:
    kind: iterativeSql
    input: companies
    sql: |
      SELECT
        COALESCE(parent.tree_id, c.tree_id) AS tree_id,
        c.parent_company_number,
        c.company_number
      FROM companies c
      LEFT JOIN __this__ parent
      ON c.parent_company_number = parent.company_number
```
Within the first step, the output of the input mapping `companies` is assigned to the identifier `__this__`. Then the
SQL query is executed for the first time, which will provide the start value of the forthcoming iteration. In each
iteration, the result of the previous iteration is assigned to `__this__` and the query is executed.
Then the result is compared to the result of the previous iteration. If the results are the same, a fix point is 
reached and the execution stops. Otherwise, the iteration will continue. 

## Fields
* `kind` **(mandatory)** *(type: string)*: `iterativeSql`

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

* `input` **(required)** *(type: string)*:
The input mapping which serves as the starting point of the iteration. This means that for the first execution, 
the identifier `__this__` will simply refer the output of this mapping. Within the next iterations, `__this__` will 
refer to the result of the previous iteration.

* `sql` **(optional)** *(type: string)* *(default: empty)*: 
The SQL statement to execute

* `file` **(optional)** *(type: string)* *(default: empty)*: 
The name of a file containing the SQL to execute.

* `uri` **(optional)** *(type: string)* *(default: empty)*: 
A URL pointing to a resource containing the SQL to execute.

* `maxIterations` **(optional)** *(type: int)* *(default: 99)*:
The maximum of iterations. The mapping will fail if the number of actual iterations required to find the fix point 
exceeds this number.


## Outputs
* `main` - the only output of the mapping


## Description
The `iterativeSql` mapping allows executing recursive SQL statements, which refer to themselves.

Flowman also supports [`recursiveSql` mappings](recursive-sql.md), which provide similar functionality more along
the lines of classical recursive SQL statements.
