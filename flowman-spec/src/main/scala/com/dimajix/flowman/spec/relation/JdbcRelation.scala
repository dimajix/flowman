/*
 * Copyright 2018-2019 Kaya Kupferschmidt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.flowman.spec.relation

import java.sql.Connection
import java.sql.SQLInvalidAuthorizationSpecException
import java.sql.SQLNonTransientConnectionException
import java.sql.SQLNonTransientException
import java.sql.Statement
import java.util.Locale

import scala.collection.mutable
import scala.util.control.NonFatal

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.PartitionAlreadyExistsException
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.common.SetIgnoreCase
import com.dimajix.common.Trilean
import com.dimajix.flowman.catalog.TableChange
import com.dimajix.flowman.catalog.TableChange.AddColumn
import com.dimajix.flowman.catalog.TableChange.DropColumn
import com.dimajix.flowman.catalog.TableChange.UpdateColumnComment
import com.dimajix.flowman.catalog.TableChange.UpdateColumnNullability
import com.dimajix.flowman.catalog.TableChange.UpdateColumnType
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MigrationFailedException
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.UnspecifiedSchemaException
import com.dimajix.flowman.jdbc.JdbcUtils
import com.dimajix.flowman.jdbc.SqlDialect
import com.dimajix.flowman.jdbc.SqlDialects
import com.dimajix.flowman.jdbc.TableDefinition
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.ConnectionIdentifier
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.PartitionSchema
import com.dimajix.flowman.model.PartitionedRelation
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.SchemaRelation
import com.dimajix.flowman.spec.connection.JdbcConnection
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.{StructType => FlowmanStructType}
import com.dimajix.spark.sql.SchemaUtils


case class JdbcRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema],
    override val partitions: Seq[PartitionField],
    connection: ConnectionIdentifier,
    properties: Map[String,String] = Map(),
    database: Option[String] = None,
    table: Option[String] = None,
    query: Option[String] = None
) extends BaseRelation with PartitionedRelation with SchemaRelation {
    private val logger = LoggerFactory.getLogger(classOf[JdbcRelation])
    private lazy val jdbcConnection = context.getConnection(connection).asInstanceOf[JdbcConnection]

    def tableIdentifier : TableIdentifier = TableIdentifier(table.getOrElse(""), database)


    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    override def provides: Set[ResourceIdentifier] = {
        table.map(t => ResourceIdentifier.ofJdbcTable(t, database)).toSet
    }

    /**
      * Returns the list of all resources which will be required by this relation for creation.
      *
      * @return
      */
    override def requires: Set[ResourceIdentifier] = {
        database.map(db => ResourceIdentifier.ofJdbcDatabase(db)).toSet
    }

    /**
      * Returns the list of all resources which will are managed by this relation for reading or writing a specific
      * partition. The list will be specifically  created for a specific partition, or for the full relation (when the
      * partition is empty)
      *
      * @param partitions
      * @return
      */
    override def resources(partitions: Map[String, FieldValue]): Set[ResourceIdentifier] = {
        require(partitions != null)

        requireValidPartitionKeys(partitions)

        val allPartitions = PartitionSchema(this.partitions).interpolate(partitions)
        allPartitions.map(p => ResourceIdentifier.ofJdbcTablePartition(table.getOrElse(""), database, p.toMap)).toSet
    }

    /**
      * Reads the configured table from the source
      * @param execution
      * @param schema
      * @return
      */
    override def read(execution:Execution, schema:Option[StructType], partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        require(execution != null)
        require(schema != null)
        require(partitions != null)

        // Get Connection
        val (_,props) = createProperties()

        // Read from database. We do not use this.reader, because Spark JDBC sources do not support explicit schemas
        val reader = execution.spark.read
            .format("jdbc")
            .options(props)

        val tableDf =
            if (query.nonEmpty) {
                logger.info(s"Reading JDBC relation '$identifier' with a custom query via connection '$connection' partition $partitions")
                reader.option(JDBCOptions.JDBC_QUERY_STRING, query.get)
                    .load()
            }
            else {
                logger.info(s"Reading JDBC relation '$identifier' from table $tableIdentifier via connection '$connection' partition $partitions")
                reader.option(JDBCOptions.JDBC_TABLE_NAME, tableIdentifier.unquotedString)
                    .load()
            }

        // Apply embedded schema, if it is specified. This will remove/cast any columns not present in the
        // explicit schema specification of the relation
        val schemaDf = applyInputSchema(tableDf)

        val df = filterPartition(schemaDf, partitions)
        SchemaUtils.applySchema(df, schema)
    }

    /**
      * Writes a given DataFrame into a JDBC connection
      *
      * @param execution
      * @param df
      * @param partition
      * @param mode
      */
    override def write(execution:Execution, df:DataFrame, partition:Map[String,SingleValue], mode:OutputMode) : Unit = {
        require(execution != null)
        require(df != null)
        require(partition != null)

        logger.info(s"Writing JDBC relation '$identifier' for table $tableIdentifier using connection '$connection' partition $partition with mode '$mode'")
        if (query.nonEmpty)
            throw new UnsupportedOperationException(s"Cannot write into JDBC relation '$identifier' which is defined by an SQL query")

        // Apply schema and add partition column
        val dfExt = addPartition(applyOutputSchema(execution, df), partition)

        // Write partition into DataBase
        if (partition.isEmpty) {
            doWrite(execution, dfExt, mode.batchMode)
        }
        else {
            mode match {
                case OutputMode.OVERWRITE =>
                    withStatement { (statement, options) =>
                        val dialect = SqlDialects.get(options.url)
                        val condition = partitionCondition(dialect, partition)
                        val query = "DELETE FROM " + dialect.quote(tableIdentifier) + " WHERE " + condition
                        statement.executeUpdate(query)
                    }
                    doWrite(execution, dfExt, SaveMode.Append)
                case OutputMode.APPEND =>
                    doWrite(execution, dfExt, SaveMode.Append)
                case OutputMode.UPDATE =>
                    ???
                case OutputMode.IGNORE_IF_EXISTS =>
                    if (!checkPartition(partition)) {
                        doWrite(execution, dfExt, SaveMode.Append)
                    }
                case OutputMode.ERROR_IF_EXISTS =>
                    if (!checkPartition(partition)) {
                        doWrite(execution, dfExt, SaveMode.Append)
                    }
                    else {
                        throw new PartitionAlreadyExistsException(database.getOrElse(""), table.get, partition.mapValues(_.value))
                    }
                case _ => throw new IllegalArgumentException(s"Unsupported save mode: '$mode'. " +
                    "Accepted save modes are 'overwrite', 'append', 'ignore', 'error', 'errorifexists'.")
            }
        }
    }
    private def doWrite(execution: Execution, df:DataFrame ,saveMode: SaveMode): Unit = {
        val (_,props) = createProperties()
        this.writer(execution, df, "jdbc", Map(), saveMode)
            .options(props)
            .option(JDBCOptions.JDBC_TABLE_NAME, tableIdentifier.unquotedString)
            .save()
    }

    /**
      * Removes one or more partitions.
      * @param execution
      * @param partitions
      */
    override def truncate(execution: Execution, partitions: Map[String, FieldValue]): Unit = {
        require(execution != null)
        require(partitions != null)

        if (query.nonEmpty)
            throw new UnsupportedOperationException(s"Cannot clean JDBC relation '$identifier' which is defined by an SQL query")

        if (partitions.isEmpty) {
            logger.info(s"Cleaning JDBC relation '$identifier', this will truncate JDBC table $tableIdentifier")
            withConnection { (con, options) =>
                JdbcUtils.truncateTable(con, tableIdentifier, options)
            }
        }
        else {
            logger.info(s"Cleaning partitions of JDBC relation '$identifier', this will partially truncate JDBC table $tableIdentifier")
            withStatement { (statement, options) =>
                val dialect = SqlDialects.get(options.url)
                val condition = partitionCondition(dialect, partitions)
                val query = "DELETE FROM " + dialect.quote(tableIdentifier) + " WHERE " + condition
                statement.executeUpdate(query)
            }
        }
    }

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage
      * @param execution
      * @return
      */
    override def exists(execution:Execution) : Trilean = {
        require(execution != null)

        withConnection{ (con,options) =>
            JdbcUtils.tableExists(con, tableIdentifier, options)
        }
    }


    /**
     * Returns true if the target partition exists and contains valid data. Absence of a partition indicates that a
     * [[write]] is required for getting up-to-date contents. A [[write]] with output mode
     * [[OutputMode.ERROR_IF_EXISTS]] then should not throw an error but create the corresponding partition
     *
     * @param execution
     * @param partition
     * @return
     */
    override def loaded(execution: Execution, partition: Map[String, SingleValue]): Trilean = {
        require(execution != null)
        require(partition != null)

        withConnection{ (con,options) =>
            val dialect = SqlDialects.get(options.url)
            val condition = partitionCondition(dialect, partition)

            JdbcUtils.tableExists(con, tableIdentifier, options) &&
                !JdbcUtils.emptyResult(con, tableIdentifier, condition, options)
        }
    }

    /**
      * This method will physically create the corresponding relation in the target JDBC database.
     *
     * @param execution
      */
    override def create(execution:Execution, ifNotExists:Boolean=false) : Unit = {
        require(execution != null)

        if (query.nonEmpty)
            throw new UnsupportedOperationException(s"Cannot create JDBC relation '$identifier' which is defined by an SQL query")

        withConnection{ (con,options) =>
            if (!ifNotExists || !JdbcUtils.tableExists(con, tableIdentifier, options)) {
                doCreate(con, options)
            }
        }
    }

    private def doCreate(con:Connection, options:JDBCOptions): Unit = {
        logger.info(s"Creating JDBC relation '$identifier', this will create JDBC table $tableIdentifier with schema\n${this.schema.map(_.treeString).orNull}")
        if (this.schema.isEmpty) {
            throw new UnspecifiedSchemaException(identifier)
        }
        val schema = this.schema.get
        val table = TableDefinition(
            tableIdentifier,
            schema.fields ++ partitions.map(_.field),
            schema.description,
            schema.primaryKey
        )
        JdbcUtils.createTable(con, table, options)
    }

    /**
      * This method will physically destroy the corresponding relation in the target JDBC database.
      * @param execution
      */
    override def destroy(execution:Execution, ifExists:Boolean=false) : Unit = {
        require(execution != null)

        if (query.nonEmpty)
            throw new UnsupportedOperationException(s"Cannot destroy JDBC relation '$identifier' which is defined by an SQL query")

        logger.info(s"Destroying JDBC relation '$identifier', this will drop JDBC table $tableIdentifier")
        withConnection{ (con,options) =>
            if (!ifExists || JdbcUtils.tableExists(con, tableIdentifier, options)) {
                JdbcUtils.dropTable(con, tableIdentifier, options)
            }
        }
    }

    override def migrate(execution:Execution, migrationPolicy:MigrationPolicy, migrationStrategy:MigrationStrategy) : Unit = {
        if (query.nonEmpty)
            throw new UnsupportedOperationException(s"Cannot create JDBC relation '$identifier' which is defined by an SQL query")

        // Only try migration if schema is explicitly specified
        if (schema.isDefined) {
            withConnection { (con, options) =>
                val dialect = SqlDialects.get(options.url)
                def getJdbcType(field:Field) : String = {
                    dialect.getJdbcType(field.ftype).databaseTypeDefinition
                }

                if (JdbcUtils.tableExists(con, tableIdentifier, options)) {
                    // Map all fields to JDBC field definitions and sort field names
                    val targetSchema = FlowmanStructType(schema.get.fields)
                    val curJdbcSchema = JdbcUtils.getJdbcSchema(con, tableIdentifier, options)
                    val curJdbcFields = curJdbcSchema.map(field => (field.name.toLowerCase(Locale.ROOT), field.typeName.toUpperCase(Locale.ROOT), field.nullable)).sorted
                    val tgtJdbcFields = targetSchema.fields.map(field => (field.name.toLowerCase(Locale.ROOT), getJdbcType(field).toUpperCase(Locale.ROOT), field.nullable)).sorted

                    if (curJdbcFields != tgtJdbcFields) {
                        val currentSchema = JdbcUtils.getSchema(curJdbcSchema, dialect)
                        doMigration(execution, currentSchema, targetSchema, migrationPolicy, migrationStrategy)
                    }
                }
            }
        }
    }

    private def doMigration(execution:Execution, currentSchema:FlowmanStructType, targetSchema:FlowmanStructType, migrationPolicy:MigrationPolicy, migrationStrategy:MigrationStrategy) : Unit = {
        withConnection { (con, options) =>
            migrationStrategy match {
                case MigrationStrategy.NEVER =>
                    logger.warn(s"Migration required for relation '$identifier', but migrations are disabled.")
                case MigrationStrategy.FAIL =>
                    throw new MigrationFailedException(identifier)
                case MigrationStrategy.ALTER =>
                    val migrations = TableChange.migrate(currentSchema, targetSchema, migrationPolicy)
                    if (migrations.exists(m => !supported(m))) {
                        logger.error(s"Cannot migrate relation HiveTable '$identifier' of Hive table $tableIdentifier, since that would require unsupported changes")
                        throw new MigrationFailedException(identifier)
                    }
                    alter(migrations, con, options)
                case MigrationStrategy.ALTER_REPLACE =>
                    val migrations = TableChange.migrate(currentSchema, targetSchema, migrationPolicy)
                    if (migrations.forall(m => supported(m))) {
                        try {
                            alter(migrations, con, options)
                        }
                        catch {
                            case e:SQLNonTransientConnectionException => throw e
                            case e:SQLInvalidAuthorizationSpecException => throw e
                            case _:SQLNonTransientException => recreate(con,options)
                        }
                    }
                    else {
                        recreate(con, options)
                    }
                case MigrationStrategy.REPLACE =>
                    recreate(con, options)
            }
        }

        def alter(migrations:Seq[TableChange], con:Connection, options:JDBCOptions) : Unit = {
            logger.info(s"Migrating JDBC relation '$identifier', this will alter JDBC table $tableIdentifier. New schema:\n${targetSchema.treeString}")

            try {
                execution.catalog.alterTable(tableIdentifier, migrations)
            }
            catch {
                case NonFatal(ex) => throw new MigrationFailedException(identifier, ex)
            }
        }

        def recreate(con:Connection, options:JDBCOptions) : Unit = {
            try {
                logger.info(s"Migrating JDBC relation '$identifier', this will recreate JDBC table $tableIdentifier. New schema:\n${targetSchema.treeString}")
                JdbcUtils.dropTable(con, tableIdentifier, options)
                doCreate(con, options)
            }
            catch {
                case NonFatal(ex) => throw new MigrationFailedException(identifier, ex)
            }
        }

        def supported(change:TableChange) : Boolean = {
            change match {
                case _:DropColumn => true
                case a:AddColumn => a.column.nullable // Only allow nullable columns to be added
                case _:UpdateColumnNullability => true
                case _:UpdateColumnType => true
                case _:UpdateColumnComment => true
                case x:TableChange => throw new UnsupportedOperationException(s"Table change ${x} not supported")
            }
        }
    }

    /**
      * Creates a Spark schema from the list of fields. This JDBC implementation will add partition columns, since
      * these will be present while reading.
      * @return
      */
    override protected def inputSchema : Option[StructType] = {
        schema.map { s =>
            val partitions = this.partitions
            val partitionFields = SetIgnoreCase(partitions.map(_.name))
            StructType(s.fields.map(_.sparkField).filter(f => !partitionFields.contains(f.name)) ++ partitions.map(_.sparkField))
        }
    }

    /**
      * Creates a Spark schema from the list of fields. The list is used for output operations, i.e. for writing.
      * This JDBC implementation will infer the curren schema from the database.
      * @return
      */
    override protected def outputSchema(execution:Execution) : Option[StructType] = {
        // Always use the current schema
        withConnection { (con, options) =>
            Some(JdbcUtils.getSchema(con, tableIdentifier, options).sparkType)
        }
    }

    private def createProperties() = {
        val connection = jdbcConnection
        val props = mutable.Map[String,String]()
        props.put(JDBCOptions.JDBC_URL, connection.url)
        props.put(JDBCOptions.JDBC_DRIVER_CLASS, connection.driver)
        connection.username.foreach(props.put("user", _))
        connection.password.foreach(props.put("password", _))

        connection.properties.foreach(kv => props.put(kv._1, kv._2))
        properties.foreach(kv => props.put(kv._1, kv._2))

        (jdbcConnection.url,props.toMap)
    }

    private def withConnection[T](fn:(Connection,JDBCOptions) => T) : T = {
        val (url,props) = createProperties()
        logger.debug(s"Connecting to jdbc source at $url")

        val options = new JDBCOptions(url, tableIdentifier.unquotedString, props)
        val conn = try {
            JdbcUtils.createConnection(options)
        } catch {
            case NonFatal(e) =>
                logger.error(s"Error connecting to jdbc source at $url: ${e.getMessage}")
                throw e
        }

        try {
            fn(conn, options)
        }
        finally {
            conn.close()
        }
    }

    private def withStatement[T](fn:(Statement,JDBCOptions) => T) : T = {
        withConnection { (con, options) =>
            val statement = con.createStatement()
            try {
                statement.setQueryTimeout(JdbcUtils.queryTimeout(options))
                fn(statement, options)
            }
            finally {
                statement.close()
            }
        }
    }

    private def checkPartition(partition:Map[String,SingleValue]) : Boolean = {
        withConnection{ (connection, options) =>
            val dialect = SqlDialects.get(options.url)
            val condition = partitionCondition(dialect, partition)
            !JdbcUtils.emptyResult(connection, tableIdentifier, condition, options)
        }
    }

    private def partitionCondition(dialect:SqlDialect, partitions: Map[String, FieldValue]) : String = {
        val partitionSchema = PartitionSchema(this.partitions)
        partitions.map { case (name, value) =>
            val field = partitionSchema.get(name)
            dialect.expr.in(field.name, field.interpolate(value))
        }
        .mkString(" AND ")
    }
}




class JdbcRelationSpec extends RelationSpec with PartitionedRelationSpec with SchemaRelationSpec {
    @JsonProperty(value = "connection", required = true) private var connection: String = _
    @JsonProperty(value = "properties", required = false) private var properties: Map[String, String] = Map()
    @JsonProperty(value = "database", required = false) private var database: Option[String] = None
    @JsonProperty(value = "table", required = false) private var table: Option[String] = None
    @JsonProperty(value = "query", required = false) private var query: Option[String] = None

    /**
      * Creates the instance of the specified Relation with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): JdbcRelation = {
        JdbcRelation(
            instanceProperties(context),
            schema.map(_.instantiate(context)),
            partitions.map(_.instantiate(context)),
            ConnectionIdentifier.parse(context.evaluate(connection)),
            context.evaluate(properties),
            database.map(context.evaluate).filter(_.nonEmpty),
            table.map(context.evaluate).filter(_.nonEmpty),
            query.map(context.evaluate).filter(_.nonEmpty)
        )
    }
}
