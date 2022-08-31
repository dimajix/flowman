# JDBC Command Target

The `jdbcCommand` target is a generic and flexible execution target which allows you to execute arbitrary SQL
statements in the syntax of the target database. This way you can use database features, which are currently not
directly supported by Flowman. We highly recommend to use this only as a workaround for mitigating missing
Flowman features or for using exotic database features, since Flowman has no way of understanding what you are
doing.

## Example
The following example will create and manage a MS SQL Server full text catalog and index:
```yaml
targets:
  fulltext-catalog:
    kind: jdbcCommand
    connection: sql_server
    # Create Fulltext Catalog
    create:
      # Check that catalog does not already exists
      condition: |
        SELECT 1 FROM sys.fulltext_catalogs
        WHERE name = 'ftcat'
        HAVING COUNT(*) = 0
      sql: |
        CREATE FULLTEXT CATALOG ftcat
    # Remove fulltext catalog
    destroy:
      # Check that catalog really exists
      condition: |
        SELECT 1 FROM sys.fulltext_catalogs
        WHERE name = 'ftcat'
        HAVING COUNT(*) = 1
      sql: |
        DROP FULLTEXT CATALOG ftcat

  tweets-index:
    kind: jdbcCommand
    connection: sql_server
    # We require both the fulltext catalog and the base table
    after:
      - fulltext-catalog
      - tweets-mssql
    # Create Index
    create:
      # Check that index does nto already exist
      condition: |
        SELECT 1 FROM sys.fulltext_indexes i
        WHERE i.object_id = OBJECT_ID('dbo.tweets')
        HAVING COUNT(*) = 0
      sql: |
        CREATE FULLTEXT INDEX ON dbo.tweets
        (
          text,
          user_description
        )
        KEY INDEX PK_tweets_id ON ftcat
        WITH CHANGE_TRACKING OFF
    # Fill index by starting background indexing process
    build:
      sql: |
        ALTER FULLTEXT INDEX ON dbo.tweets START FULL POPULATION
    # Delete index
    destroy:
      # Check that index really exists
      condition: |
        SELECT 1 FROM sys.fulltext_indexes i
        WHERE i.object_id = OBJECT_ID('dbo.tweets')
        HAVING COUNT(*) = 1
      sql: |
        DROP FULLTEXT INDEX ON dbo.tweets
```
As you can see, the `jdbcCommand` target supports different SQL commands for each lifecycle phase 
(`CREATE`, `BUILD`, ...) and you can also specify an optional `condition` which serves as a precondition when the
corresponding SQL should be executed or not. This way you can avoid creating tables multiple times etc by first 
checking if the table already exists.


## Fields

* `kind` **(mandatory)** *(type: string)*: `hiveDatabase`

* `description` **(optional)** *(type: string)*:
  Optional descriptive text of the build target

* `validate` **(optional)** *(type: action)*:
  Optional action to be executed during `VALIDATE` phase

* `create` **(optional)** *(type: action)*:
  Optional action to be executed during `CREATE` phase

* `build` **(optional)** *(type: action)*:
  Optional action to be executed during `BUILD` phase

* `verify` **(optional)** *(type: action)*:
  Optional action to be executed during `VERIFY` phase

* `truncate` **(optional)** *(type: action)*:
  Optional action to be executed during `TRUNCATE` phase

* `destroy` **(optional)** *(type: action)*:
  Optional action to be executed during `DESTROY` phase

Each action has the following fields:

* `sql` **(required)** *(type: string)*:
The SQL command to be executed

* `condition` **(optional)** *(type: string)*:
An optional SQL query which is used to determine if the execution needs to be executed. The query should return a
single row with a single integer column. If that column contains a value of `1` or larger, then the target is considered 
to be in a *dirty* state and the action will be executed. Otherwise, it will be skipped. If no row is returned, the 
target is considered to be in *clean* state, and the action will be skipped.


## Supported Execution Phases
* `VALIDATE` - The action specified in `validate` will be executed.
* `CREATE` - The action specified in `create` will be executed.
* `BUILD` - The action specified in `build` will be executed.
* `VERIFY` - The action specified in `verify` will be executed.
* `TRUNCATE` - The action specified in `truncate` will be executed.
* `DESTROY` - The action specified in `destroy` will be executed.

Read more about [execution phases](../../concepts/lifecycle.md).
