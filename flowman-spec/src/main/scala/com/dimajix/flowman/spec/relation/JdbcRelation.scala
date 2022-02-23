/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

import java.sql.SQLInvalidAuthorizationSpecException
import java.sql.SQLNonTransientConnectionException
import java.sql.SQLNonTransientException
import java.sql.Statement
import java.util.Locale

import scala.collection.mutable
import scala.util.control.NonFatal

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.PartitionAlreadyExistsException
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.common.SetIgnoreCase
import com.dimajix.common.Trilean
import com.dimajix.flowman.catalog.TableChange
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.DeleteClause
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.InsertClause
import com.dimajix.flowman.execution.MergeClause
import com.dimajix.flowman.execution.MigrationFailedException
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.UnspecifiedSchemaException
import com.dimajix.flowman.execution.UpdateClause
import com.dimajix.flowman.jdbc.JdbcUtils
import com.dimajix.flowman.jdbc.SqlDialect
import com.dimajix.flowman.jdbc.SqlDialects
import com.dimajix.flowman.jdbc.TableDefinition
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.PartitionSchema
import com.dimajix.flowman.model.PartitionedRelation
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.SchemaRelation
import com.dimajix.flowman.spec.connection.ConnectionReferenceSpec
import com.dimajix.flowman.spec.connection.JdbcConnection
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.{StructType => FlowmanStructType}


class JdbcRelationBase(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema] = None,
    override val partitions: Seq[PartitionField] = Seq(),
    connection: Reference[Connection],
    properties: Map[String,String] = Map(),
    database: Option[String] = None,
    table: Option[String] = None,
    query: Option[String] = None,
    mergeKey: Seq[String] = Seq(),
    primaryKey: Seq[String] = Seq()
) extends BaseRelation with PartitionedRelation with SchemaRelation {
    protected val logger = LoggerFactory.getLogger(getClass)
    protected val tableIdentifier : TableIdentifier = TableIdentifier(table.getOrElse(""), database)

    if (query.nonEmpty && table.nonEmpty)
        throw new IllegalArgumentException(s"JDBC relation '$identifier' cannot have both a table and a SQL query defined")
    if (query.isEmpty && table.isEmpty)
        throw new IllegalArgumentException(s"JDBC relation '$identifier' needs either a table or a SQL query defined")


    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    override def provides: Set[ResourceIdentifier] = {
        // Only return a resource if a table is defined, which implies that this relation can be used for creating
        // and destroying JDBC tables
        table.map(t => ResourceIdentifier.ofJdbcTable(t, database)).toSet
    }

    /**
      * Returns the list of all resources which will be required by this relation for creation.
      *
      * @return
      */
    override def requires: Set[ResourceIdentifier] = {
        // Only return a resource if a table is defined, which implies that this relation can be used for creating
        // and destroying JDBC tables
        database.map(db => ResourceIdentifier.ofJdbcDatabase(db)).toSet ++ super.requires
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

        if (query.nonEmpty) {
            Set(ResourceIdentifier.ofJdbcQuery(query.get))
        }
        else {
            val allPartitions = PartitionSchema(this.partitions).interpolate(partitions)
            allPartitions.map(p => ResourceIdentifier.ofJdbcTablePartition(table.get, database, p.toMap)).toSet
        }
    }

    /**
     * Returns the schema of the relation, either from an explicitly specified schema or by schema inference from
     * the physical source
     * @param execution
     * @return
     */
    override def describe(execution:Execution, partitions:Map[String,FieldValue] = Map()) : FlowmanStructType = {
        val result = if (schema.nonEmpty) {
            FlowmanStructType(fields)
        }
        else {
            withConnection { (con, options) =>
                JdbcUtils.getSchema(con, tableIdentifier, options)
            }
        }

        applyDocumentation(result)
    }

    /**
      * Reads the configured table from the source
      * @param execution
      * @return
      */
    override def read(execution:Execution, partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        require(execution != null)
        require(partitions != null)

        // Get Connection
        val (_,props) = createConnectionProperties()

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

        val filteredDf = filterPartition(tableDf, partitions)

        // Apply embedded schema, if it is specified. This will remove/cast any columns not present in the
        // explicit schema specification of the relation
        //SchemaUtils.applySchema(filteredDf, inputSchema, insertNulls=false)
        applyInputSchema(execution, filteredDf, includePartitions = false)
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
        mode match {
            case OutputMode.OVERWRITE if partition.isEmpty =>
                doOverwriteAll(execution, dfExt)
            case OutputMode.OVERWRITE =>
                doOverwritePartition(execution, dfExt, partition)
            case OutputMode.APPEND =>
                doAppend(execution, dfExt)
            case OutputMode.IGNORE_IF_EXISTS =>
                if (!checkPartition(partition)) {
                    doAppend(execution, dfExt)
                }
            case OutputMode.ERROR_IF_EXISTS =>
                if (!checkPartition(partition)) {
                    doAppend(execution, dfExt)
                }
                else {
                    throw new PartitionAlreadyExistsException(database.getOrElse(""), table.get, partition.mapValues(_.value))
                }
            case OutputMode.UPDATE =>
                doUpdate(execution, df)
            case _ => throw new IllegalArgumentException(s"Unsupported save mode: '$mode'. " +
                "Accepted save modes are 'overwrite', 'append', 'ignore', 'error', 'update', 'errorifexists'.")
        }
    }
    protected def doOverwriteAll(execution: Execution, df:DataFrame) : Unit = {
        withConnection { (con, options) =>
            JdbcUtils.truncateTable(con, tableIdentifier, options)
        }
        doAppend(execution, df)
    }
    protected def doOverwritePartition(execution: Execution, df:DataFrame, partition:Map[String,SingleValue]) : Unit = {
        withStatement { (statement, options) =>
            val dialect = SqlDialects.get(options.url)
            val condition = partitionCondition(dialect, partition)
            val query = "DELETE FROM " + dialect.quote(tableIdentifier) + " WHERE " + condition
            statement.executeUpdate(query)
        }
        doAppend(execution, df)
    }
    protected def doAppend(execution: Execution, df:DataFrame): Unit = {
        val (_,props) = createConnectionProperties()
        this.writer(execution, df, "jdbc", Map(), SaveMode.Append)
            .options(props)
            .option(JDBCOptions.JDBC_TABLE_NAME, tableIdentifier.unquotedString)
            .save()
    }
    protected def doUpdate(execution: Execution, df:DataFrame): Unit = {
        throw new IllegalArgumentException(s"Unsupported save mode: 'UPDATE' for generic JDBC relation.")
    }

    /**
     * Performs a merge operation. Either you need to specify a [[mergeKey]], or the relation needs to provide some
     * default key.
     *
     * @param execution
     * @param df
     * @param mergeCondition
     * @param clauses
     */
    override def merge(execution: Execution, df: DataFrame, condition:Option[Column], clauses: Seq[MergeClause]): Unit = {
        logger.info(s"Writing JDBC relation '$identifier' for table $tableIdentifier using connection '$connection' using merge operation")
        if (query.nonEmpty)
            throw new UnsupportedOperationException(s"Cannot write into JDBC relation '$identifier' which is defined by an SQL query")

        val mergeCondition =
            condition.getOrElse {
                val withinPartitionKeyColumns =
                    if (mergeKey.nonEmpty)
                        mergeKey
                    else if (primaryKey.nonEmpty)
                        primaryKey
                    else if (schema.exists(_.primaryKey.nonEmpty))
                        schema.map(_.primaryKey).get
                    else
                        throw new IllegalArgumentException(s"Merging JDBC relation '$identifier' requires primary key in schema, explicit merge key or merge condition")
                (SetIgnoreCase(partitions.map(_.name)) ++ withinPartitionKeyColumns)
                    .toSeq
                    .map(c => col("source." + c) === col("target." + c))
                    .reduce(_ && _)
            }

        val sourceColumns = collectColumns(mergeCondition.expr, "source") ++ clauses.flatMap(c => collectColumns(df.schema, c, "source"))
        val sourceDf = df.select(sourceColumns.toSeq.map(col):_*)

        val (url, props) = createConnectionProperties()
        val options = new JDBCOptions(url, tableIdentifier.unquotedString, props)
        val targetSchema = outputSchema(execution)
        JdbcUtils.mergeTable(tableIdentifier, "target", targetSchema, sourceDf, "source", mergeCondition, clauses, options)
    }

    /**
      * Removes one or more partitions.
     *
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

        if (query.nonEmpty) {
            true
        }
        else {
            withConnection { (con, options) =>
                JdbcUtils.tableExists(con, tableIdentifier, options)
            }
        }
    }

    /**
     * Returns true if the relation exists and has the correct schema. If the method returns false, but the
     * relation exists, then a call to [[migrate]] should result in a conforming relation.
     *
     * @param execution
     * @return
     */
    override def conforms(execution: Execution, migrationPolicy: MigrationPolicy): Trilean = {
        // Only try migration if schema is explicitly specified
        if (query.nonEmpty) {
            true
        }
        else {
            withConnection { (con, options) =>
                if (JdbcUtils.tableExists(con, tableIdentifier, options)) {
                    if (schema.nonEmpty) {
                        val targetSchema = fullSchema.get
                        val currentSchema = JdbcUtils.getSchema(con, tableIdentifier, options)
                        !TableChange.requiresMigration(currentSchema, targetSchema, migrationPolicy)
                    }
                    else {
                        true
                    }
                }
                else {
                    false
                }
            }
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
                provides.foreach(execution.refreshResource)
            }
        }
    }

    protected def doCreate(con:java.sql.Connection, options:JDBCOptions): Unit = {
        logger.info(s"Creating JDBC relation '$identifier', this will create JDBC table $tableIdentifier with schema\n${this.schema.map(_.treeString).orNull}")
        if (this.schema.isEmpty) {
            throw new UnspecifiedSchemaException(identifier)
        }
        val schema = this.schema.get
        val table = TableDefinition(
            tableIdentifier,
            schema.fields ++ partitions.map(_.field),
            schema.description,
            if (primaryKey.nonEmpty) primaryKey else schema.primaryKey
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
                provides.foreach(execution.refreshResource)
            }
        }
    }

    override def migrate(execution:Execution, migrationPolicy:MigrationPolicy, migrationStrategy:MigrationStrategy) : Unit = {
        if (query.nonEmpty)
            throw new UnsupportedOperationException(s"Cannot migrate JDBC relation '$identifier' which is defined by an SQL query")

        // Only try migration if schema is explicitly specified
        if (schema.isDefined) {
            withConnection { (con, options) =>
                if (JdbcUtils.tableExists(con, tableIdentifier, options)) {
                    val targetSchema = fullSchema.get
                    val currentSchema = JdbcUtils.getSchema(con, tableIdentifier, options)
                    if (TableChange.requiresMigration(currentSchema, targetSchema, migrationPolicy)) {
                        doMigration(currentSchema, targetSchema, migrationPolicy, migrationStrategy)
                        provides.foreach(execution.refreshResource)
                    }
                }
            }
        }
    }

    private def doMigration(currentSchema:FlowmanStructType, targetSchema:FlowmanStructType, migrationPolicy:MigrationPolicy, migrationStrategy:MigrationStrategy) : Unit = {
        withConnection { (con, options) =>
            migrationStrategy match {
                case MigrationStrategy.NEVER =>
                    logger.warn(s"Migration required for relation '$identifier', but migrations are disabled.\nCurrent schema:\n${currentSchema.treeString}New schema:\n${targetSchema.treeString}")
                case MigrationStrategy.FAIL =>
                    logger.error(s"Cannot migrate relation '$identifier', but migrations are disabled.\nCurrent schema:\n${currentSchema.treeString}New schema:\n${targetSchema.treeString}")
                    throw new MigrationFailedException(identifier)
                case MigrationStrategy.ALTER =>
                    val dialect = SqlDialects.get(options.url)
                    val migrations = TableChange.migrate(currentSchema, targetSchema, migrationPolicy)
                    if (migrations.exists(m => !dialect.supportsChange(tableIdentifier, m))) {
                        logger.error(s"Cannot migrate relation JDBC relation '$identifier' of table $tableIdentifier, since that would require unsupported changes.\nCurrent schema:\n${currentSchema.treeString}New schema:\n${targetSchema.treeString}")
                        throw new MigrationFailedException(identifier)
                    }
                    alter(migrations, con, options)
                case MigrationStrategy.ALTER_REPLACE =>
                    val dialect = SqlDialects.get(options.url)
                    val migrations = TableChange.migrate(currentSchema, targetSchema, migrationPolicy)
                    if (migrations.forall(m => dialect.supportsChange(tableIdentifier, m))) {
                        try {
                            alter(migrations, con, options)
                        }
                        catch {
                            case e: SQLNonTransientConnectionException => throw e
                            case e: SQLInvalidAuthorizationSpecException => throw e
                            case _: SQLNonTransientException => recreate(con, options)
                        }
                    }
                    else {
                        recreate(con, options)
                    }
                case MigrationStrategy.REPLACE =>
                    recreate(con, options)
            }
        }

        def alter(migrations:Seq[TableChange], con:java.sql.Connection, options:JDBCOptions) : Unit = {
            logger.info(s"Migrating JDBC relation '$identifier', this will alter JDBC table $tableIdentifier. New schema:\n${targetSchema.treeString}")
            if (migrations.isEmpty)
                logger.warn("Empty list of migrations - nothing to do")

            try {
                JdbcUtils.alterTable(con, tableIdentifier, migrations, options)
            }
            catch {
                case NonFatal(ex) => throw new MigrationFailedException(identifier, ex)
            }
        }

        def recreate(con:java.sql.Connection, options:JDBCOptions) : Unit = {
            try {
                logger.info(s"Migrating JDBC relation '$identifier', this will recreate JDBC table $tableIdentifier. New schema:\n${targetSchema.treeString}")
                JdbcUtils.dropTable(con, tableIdentifier, options)
                doCreate(con, options)
            }
            catch {
                case NonFatal(ex) => throw new MigrationFailedException(identifier, ex)
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

    protected def createConnectionProperties() : (String,Map[String,String]) = {
        val connection = this.connection.value.asInstanceOf[JdbcConnection]
        val props = mutable.Map[String,String]()
        props.put(JDBCOptions.JDBC_URL, connection.url)
        props.put(JDBCOptions.JDBC_DRIVER_CLASS, connection.driver)
        connection.username.foreach(props.put("user", _))
        connection.password.foreach(props.put("password", _))

        connection.properties.foreach(kv => props.put(kv._1, kv._2))
        properties.foreach(kv => props.put(kv._1, kv._2))

        (connection.url,props.toMap)
    }

    protected def withConnection[T](fn:(java.sql.Connection,JDBCOptions) => T) : T = {
        val (url,props) = createConnectionProperties()
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

    protected def withTransaction[T](con:java.sql.Connection)(fn: => T) : T = {
        JdbcUtils.withTransaction(con)(fn)
    }

    protected def withStatement[T](fn:(Statement,JDBCOptions) => T) : T = {
        withConnection { (con, options) =>
            withStatement(con,options)(fn)
        }
    }

    protected def withStatement[T](con:java.sql.Connection,options:JDBCOptions)(fn:(Statement,JDBCOptions) => T) : T = {
        val statement = con.createStatement()
        try {
            statement.setQueryTimeout(JdbcUtils.queryTimeout(options))
            fn(statement, options)
        }
        finally {
            statement.close()
        }
    }

    private def checkPartition(partition:Map[String,SingleValue]) : Boolean = {
        withConnection{ (connection, options) =>
            val condition = {
                if (partition.isEmpty) {
                    "1=1"
                }
                else {
                    val dialect = SqlDialects.get(options.url)
                    partitionCondition(dialect, partition)
                }
            }
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

    private def collectColumns(sourceSchema:StructType, clause:MergeClause, prefix:String) : SetIgnoreCase = {
        clause match {
            case i:InsertClause =>
                val conditionColumns = i.condition.map(c => collectColumns(c.expr, prefix)).getOrElse(SetIgnoreCase())
                val insertColumns = if(i.columns.nonEmpty) i.columns.values.flatMap(c => collectColumns(c.expr, prefix)) else sourceSchema.names.toSeq
                conditionColumns ++ insertColumns
            case u:UpdateClause =>
                val conditionColumns = u.condition.map(c => collectColumns(c.expr, prefix)).getOrElse(SetIgnoreCase())
                val updateColumns = if(u.columns.nonEmpty) u.columns.values.flatMap(c => collectColumns(c.expr, prefix)) else sourceSchema.names.toSeq
                conditionColumns ++ updateColumns
            case d:DeleteClause =>
                d.condition.map(c => collectColumns(c.expr, prefix)).getOrElse(SetIgnoreCase())
        }
    }
    private def collectColumns(expr:Expression, prefix:String) : SetIgnoreCase = {
        val lwrPrefix = prefix.toLowerCase(Locale.ROOT)
        val columns = expr.collect {
            case UnresolvedAttribute(x) if x.head.toLowerCase(Locale.ROOT) == lwrPrefix => x.tail.mkString(".")
        }
        SetIgnoreCase(columns)
    }
}


case class JdbcRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema] = None,
    override val partitions: Seq[PartitionField] = Seq(),
    connection: Reference[Connection],
    properties: Map[String,String] = Map(),
    database: Option[String] = None,
    table: Option[String] = None,
    query: Option[String] = None,
    mergeKey: Seq[String] = Seq(),
    primaryKey: Seq[String] = Seq()
) extends JdbcRelationBase(
    instanceProperties,
    schema,
    partitions,
    connection,
    properties,
    database,
    table,
    query,
    mergeKey,
    primaryKey
) {
}


class JdbcRelationSpec extends RelationSpec with PartitionedRelationSpec with SchemaRelationSpec {
    @JsonProperty(value = "connection", required = true) private var connection: ConnectionReferenceSpec = _
    @JsonProperty(value = "properties", required = false) private var properties: Map[String, String] = Map()
    @JsonProperty(value = "database", required = false) private var database: Option[String] = None
    @JsonProperty(value = "table", required = false) private var table: Option[String] = None
    @JsonProperty(value = "query", required = false) private var query: Option[String] = None
    @JsonProperty(value = "mergeKey", required = false) private var mergeKey: Seq[String] = Seq()
    @JsonProperty(value = "primaryKey", required = false) private var primaryKey: Seq[String] = Seq()

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
            connection.instantiate(context),
            context.evaluate(properties),
            database.map(context.evaluate).filter(_.nonEmpty),
            table.map(context.evaluate).filter(_.nonEmpty),
            query.map(context.evaluate).filter(_.nonEmpty),
            mergeKey.map(context.evaluate),
            primaryKey.map(context.evaluate)
        )
    }
}
