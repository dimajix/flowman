# Advanced JDBC Database Features

Flowman already provides a very robust for dealing with relation databases, both as data sources and as data sinks.
But when writing into a relational database, you eventually might find yourself in a situation where Flowman does 
not support some special or exotic features of the relational database, which you require. In this case you need
more control than provided via the standard approach by using the [`jdbcTable`](../spec/relation/jdbcTable.md), 
[`jdbcQuery`](../spec/relation/jdbcQuery.md) and [`jdbcView`](../spec/relation/jdbcView.md) relations.

Basically, there are two main situations where the abstraction provided by Flowman might hide required special features:
* You need more control over the `CREATE TABLE` statement
* You need to execute additional commands as part of your data build process

Flowman offers support for these two scenarios.


## Full Control for `CREATE TABLE` Statements

Starting with Flowman 0.27.0, you can now explicitly specify the `CREATE TABLE` statement(s) in a [`jdbcTable`](../spec/relation/jdbcTable.md)
relation, which will be used instead of Flowman's standard mechanism for assembling the SQL statements:

```yaml
relations:
  frontend_users:
    kind: jdbcTable
    # Directly embed a connection
    connection:
      kind: jdbc
      driver: "$frontend_db_driver"
      url: "$frontend_db_url"
      username: "$frontend_db_username"
      password: "$frontend_db_password"
    # Specify the table
    table: "frontend_users"
    sql: 
      - |
        CREATE TABLE dbo.frontend_users(
          "id" BIGINT,
          "description" CLOB, 
          "flags" INTEGER, 
          "name" VARCHAR(32)
        )
      - CREATE CLUSTERED COLUMNSTORE INDEX CI_frontend_users ON dbo.frontend_users
      - ALTER TABLE dbo.frontend_users ADD CONSTRAINT PK_frontend_users PRIMARY KEY NONCLUSTERED(id);
```
In this case, Flowman will only use the SQL statement for creating the table. This gives you full control, but at the
same time, completely disables automatic migrations.


## Executing arbitrary SQL Statements

In addition to giving you more control over the `CREATE TABLE` statement, since version 0.27.0 Flowman has also 
implemented a generic [`jdbcCommand` target](../spec/target/jdbcCommand.md) for executing arbitrary SQL statements.

The following example will create and manage an MS SQL Server full text catalog and index:
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
      sql:
        # Start Population
        - ALTER FULLTEXT INDEX ON dbo.tweets START FULL POPULATION
        # Wait until indexing has finished (optional)
        - |
          WHILE (FULLTEXTCATALOGPROPERTY('ftcat','PopulateStatus') = 1)
          BEGIN
              WAITFOR DELAY '00:00:03'
          END
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
corresponding SQL should be executed or not. This way you can avoid creating tables multiple times etc. by first
checking if the table already exists.
