# Schema Migrations

The topic *schema migration* refers to the process to modify an existing database schema (i.e. a collection of tables)
from their current state (in the sense of column names, data types, indexes etc.) to a desired scheme. This is a very
important topic, since it would be naive to assume that your data schema will never change. Every project eventually
will face the challenge of modifying the existing schema due to new business requirements.

There are different approaches to this topic, a very common is to use frameworks like LiquiBase or Alembic, which
would execute SQL scripts provided by developers to perform schema migrations. Flowman on the other hand offers
automatic schema migrations, where the current schema is compared with the desired schema (as specified in the
[relations](../spec/relation/index.md) of a Flowman project) and infers the required changes automatically. 

The clear advantage of Flowmans approach is its simplicity from a developers point of view. The clear disadvantage
is that Flowman conceptually cannot capture all desired changes. For example, Flowman cannot detect the intention
of renaming an existing column, instead Flowman will drop the existing column and add a new one with the new name.
Nevertheless, Flowmans approach already turns out to be a good solution for many scenarios, where backward compatibility
of the data schema is an important thing. This is true for Data Lakes, Data Meshes and other shared databases.

Automatic migrations are not supported by all relation types, since not all really support changing an existing
schema in a robust way. A typical example would be a `file` relation. But other relations well support automatic
schema migration, for example
* [`jdbcTable`](../spec/relation/jdbcTable.md)
* [`jdbcView`](../spec/relation/jdbcView.md)
* [`hiveTable`](../spec/relation/hiveTable.md)
* [`hiveView`](../spec/relation/hiveView.md)
* [`hiveUnionTable`](../spec/relation/hiveUnionTable.md)
* [`deltaFile`](../spec/relation/deltaFile.md)
* [`deltaTable`](../spec/relation/deltaTable.md)

Within each of these relation types, you can specify a *migration policy* and a *migration strategy*. If you omit
these parameters, then the Flowman configuration properties `flowman.default.relation.migrationPolicy` and
`flowman.default.relation.migrationStrategy`, see [configuration](../setup/config.md) for more details) will
be used instead.


## Migration Policy

The *migration policy* determines, when a relation needs migration. There are two possible values: `STRICT` and 
`RELAXED`. When the policy is set to `STRICT`, certain mismatches are allowed, for example
* Different data types, as long as the existing data type can hold values of the desired data type.
* Unused columns, which are currently present, but not present in the desired schema, will not be removed.
* Differences in nullability, if the desired state is stricter (i.e. not-nullable)
When the migration policy is set to `STRICT`, then these differences will also imply a migration.

In order to minimize the required migrations, the default value of the Flowman configuration property
`flowman.default.relation.migrationPolicy` is set to `RELAXED`.


## Migration Strategy

The *migration strategy* controls *how* the migration should be done. There are five possible values:
* `NEVER` - No required migration will not be performed at all, only a warning will be logged.
* `FAIL` - Any required migration will fail the build.
* `ALTER` - Flowman will try to change the schema without recreating the table. If this is not possible, the build will fail.
* `ALTER_REPLACE` - Flowman will first try to migrate the schema without recreating the table. If that is not possible,
Flowman will recreate the table instead. Note that recreating a table implies a loss of the corresponding data.
* `REPLACE` - Any require migration will lead to a recreating of the table. Note that recreating a table implies a loss
of the corresponding data.

Since recreating a table may lead to a data loss, the default value of the Flowman configuration property
`flowman.default.relation.migrationStrategy` is set to `ALTER`.


## Time of Migration

Flowman will perform all migrations during the [`CREATE` execution phase](lifecycle.md), before the target
relations are populated with new records. Pending migrations are performed for all relations used in
[build targets](../spec/target/index.md) which are listed in a [job](../spec/job/index.md).
