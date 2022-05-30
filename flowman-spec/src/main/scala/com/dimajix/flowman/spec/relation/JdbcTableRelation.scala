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
import org.apache.spark.sql.catalyst.analysis.PartitionAlreadyExistsException
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.dimajix.common.SetIgnoreCase
import com.dimajix.common.Trilean
import com.dimajix.flowman.catalog
import com.dimajix.flowman.catalog.TableChange
import com.dimajix.flowman.catalog.TableDefinition
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.catalog.TableIndex
import com.dimajix.flowman.catalog.TableType
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
import com.dimajix.flowman.jdbc.SqlDialects
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
import com.dimajix.flowman.types.SchemaUtils
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.{StructType => FlowmanStructType}


class JdbcTableRelationBase(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema] = None,
    override val partitions: Seq[PartitionField] = Seq.empty,
    connection: Reference[Connection],
    table: TableIdentifier,
    properties: Map[String,String] = Map.empty,
    mergeKey: Seq[String] = Seq.empty,
    override val primaryKey: Seq[String] = Seq.empty,
    indexes: Seq[TableIndex] = Seq.empty
) extends JdbcRelation(
    connection,
    properties + (JDBCOptions.JDBC_TABLE_NAME -> table.unquotedString)
) with SchemaRelation {
    protected val tableIdentifier: TableIdentifier = table
    protected val stagingIdentifier: Option[TableIdentifier] = None
    protected lazy val tableDefinition: Option[TableDefinition] = {
        schema.map { schema =>
            val pk = if (primaryKey.nonEmpty) primaryKey else schema.primaryKey
            val columns = fullSchema.get.fields
            TableDefinition(
                tableIdentifier,
                TableType.TABLE,
                columns=columns,
                comment=schema.description,
                primaryKey=pk,
                indexes=indexes
            )
        }
    }

    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    override def provides: Set[ResourceIdentifier] = {
        // Only return a resource if a table is defined, which implies that this relation can be used for creating
        // and destroying JDBC tables
        Set(ResourceIdentifier.ofJdbcTable(table))
    }

    /**
      * Returns the list of all resources which will be required by this relation for creation.
      *
      * @return
      */
    override def requires: Set[ResourceIdentifier] = {
        // Only return a resource if a table is defined, which implies that this relation can be used for creating
        // and destroying JDBC tables
        table.database.map(db => ResourceIdentifier.ofJdbcDatabase(db)).toSet ++ super.requires
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
        allPartitions.map(p => ResourceIdentifier.ofJdbcTablePartition(tableIdentifier, p.toMap)).toSet
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
                JdbcUtils.getTableSchema(con, tableIdentifier, options)
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

        logger.info(s"Reading JDBC relation '$identifier' from table $tableIdentifier via connection '$connection' partition $partitions")

        // Get Connection
        val (_,props) = createConnectionProperties()

        // Read from database. We do not use this.reader, because Spark JDBC sources do not support explicit schemas
        val reader = execution.spark.read
            .format("jdbc")
            .options(props)

        val tableDf = reader
            .option(JDBCOptions.JDBC_TABLE_NAME, tableIdentifier.unquotedString)
            .load()

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
                    throw new PartitionAlreadyExistsException(tableIdentifier.database.getOrElse(""), tableIdentifier.table, partition.mapValues(_.value))
                }
            case OutputMode.UPDATE =>
                doUpdate(execution, dfExt)
            case _ => throw new IllegalArgumentException(s"Unsupported save mode: '$mode'. " +
                "Accepted save modes are 'overwrite', 'append', 'ignore', 'error', 'update', 'errorifexists'.")
        }
    }
    protected def doOverwriteAll(execution: Execution, df:DataFrame) : Unit = {
        stagingIdentifier match {
            case None =>
                withConnection { (con, options) =>
                    JdbcUtils.truncateTable(con, tableIdentifier, options)
                }
                doAppend(execution, df)
            case Some(stagingTable) =>
                withConnection { (con, options) =>
                    val stagingSchema = JdbcUtils.getTableSchema(con, tableIdentifier, options)
                    createStagingTable(execution, con, options, df, Some(stagingSchema))

                    withTransaction(con) {
                        withStatement(con, options) { case (statement, options) =>
                            logger.debug(s"Truncating table ${tableIdentifier}")
                            JdbcUtils.truncateTable(statement, tableIdentifier, options)
                            logger.info(s"Copying data from staging table ${stagingTable} into table ${tableIdentifier}")
                            JdbcUtils.appendTable(statement, tableIdentifier, stagingTable, options)
                            logger.debug(s"Dropping temporary staging table ${stagingTable}")
                            JdbcUtils.dropTable(statement, stagingTable, options)
                        }
                    }
                }

        }

    }
    protected def doOverwritePartition(execution: Execution, df:DataFrame, partition:Map[String,SingleValue]) : Unit = {
        stagingIdentifier match {
            case None =>
                withStatement { (statement, options) =>
                    logger.debug(s"Truncating table ${tableIdentifier} partition ${partition}")
                    val condition = partitionCondition(partition, options)
                    JdbcUtils.truncatePartition(statement, tableIdentifier, condition, options)
                }
                doAppend(execution, df)
            case Some(stagingTable) =>
                withConnection { (con, options) =>
                    val stagingSchema = JdbcUtils.getTableSchema(con, tableIdentifier, options)
                    createStagingTable(execution, con, options, df, Some(stagingSchema))

                    withTransaction(con) {
                        withStatement(con, options) { case (statement, options) =>
                            val condition = partitionCondition(partition, options)
                            logger.debug(s"Truncating table ${tableIdentifier} partition ${partition}")
                            JdbcUtils.truncatePartition(statement, tableIdentifier, condition, options)
                            logger.info(s"Copying data from temporary staging table ${stagingTable} into table ${tableIdentifier}")
                            JdbcUtils.appendTable(statement, tableIdentifier, stagingTable, options)
                            logger.debug(s"Dropping temporary staging table ${stagingTable}")
                            JdbcUtils.dropTable(statement, stagingTable, options)
                        }
                    }
                }
        }
    }
    protected def doAppend(execution: Execution, df:DataFrame): Unit = {
        stagingIdentifier match {
            case None =>
                appendTable(execution, df, tableIdentifier)
            case Some(stagingTable) =>
                withConnection { (con, options) =>
                    val stagingSchema = JdbcUtils.getTableSchema(con, tableIdentifier, options)
                    createStagingTable(execution, con, options, df, Some(stagingSchema))

                    withTransaction(con) {
                        withStatement(con, options) { case (statement, options) =>
                            logger.info(s"Copying data from temporary staging table ${stagingTable} into table ${tableIdentifier}")
                            JdbcUtils.appendTable(statement, tableIdentifier, stagingTable, options)
                            logger.debug(s"Dropping temporary staging table ${stagingTable}")
                            JdbcUtils.dropTable(statement, stagingTable, options)
                        }
                    }
                }
        }
    }
    protected def doUpdate(execution: Execution, df:DataFrame): Unit = {
        val clauses = Seq(
            InsertClause(),
            UpdateClause()
        )

        val stagingSchema = withConnection { (con, options) =>
            JdbcUtils.getTableSchema(con, tableIdentifier, options)
        }
        doMerge(execution, df, Some(stagingSchema), mergeCondition, clauses)
    }

    private def createStagingTable(execution:Execution, con:java.sql.Connection, options: JDBCOptions, df:DataFrame, schema:Option[FlowmanStructType]) : Unit = {
        val stagingTable = this.stagingIdentifier.get
        val stagingSchema = schema.map(schema => JdbcUtils.createSchema(df.schema, schema)).getOrElse(FlowmanStructType.of(df.schema))
        logger.info(s"Creating staging table ${stagingTable} with schema\n${stagingSchema.treeString}")

        // First drop temp table if it already exists
        JdbcUtils.dropTable(con, stagingTable, options, ifExists=true)

        // Create temp table with specified schema, but without any primary key or indices
        val table = catalog.TableDefinition(
            stagingTable,
            TableType.TABLE,
            stagingSchema.fields
        )
        JdbcUtils.createTable(con, table, options)

        logger.info(s"Writing new data into temporary staging table ${stagingTable}")
        appendTable(execution, df, stagingTable)
    }

    protected def appendTable(execution: Execution, df:DataFrame, table:TableIdentifier) : Unit = {
        // Save table
        val (_,props) = createConnectionProperties()
        df.write.format("jdbc")
            .mode(SaveMode.Append)
            .options(props)
            .option(JDBCOptions.JDBC_TABLE_NAME, table.unquotedString)
            .save()
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

        val mergeCondition = condition.getOrElse(this.mergeCondition)
        val sourceColumns = collectColumns(mergeCondition.expr, "source") ++ clauses.flatMap(c => collectColumns(df.schema, c, "source"))
        val sourceDf = df.select(sourceColumns.toSeq.map(col):_*)

        doMerge(execution, sourceDf, None, mergeCondition, clauses)
    }
    protected def doMerge(execution: Execution, df: DataFrame, stagingSchema:Option[FlowmanStructType], condition:Column, clauses: Seq[MergeClause]) : Unit = {
        val targetSchema = outputSchema(execution)
        stagingIdentifier match {
            case None =>
                val (url, props) = createConnectionProperties()
                val options = new JDBCOptions(url, tableIdentifier.unquotedString, props)
                JdbcUtils.mergeTable(tableIdentifier, "target", targetSchema, df, "source", condition, clauses, options)
            case Some(stagingTable) =>
                withConnection { (con, options) =>
                    createStagingTable(execution, con, options, df, stagingSchema)

                    withTransaction(con) {
                        withStatement(con, options) { case (statement, options) =>
                            logger.info(s"Merging data from temporary staging table ${stagingTable} into table ${tableIdentifier}")
                            JdbcUtils.mergeTable(statement, tableIdentifier, "target", targetSchema, stagingTable, "source", df.schema, condition, clauses, options)
                            logger.debug(s"Dropping temporary staging table ${stagingTable}")
                            JdbcUtils.dropTable(statement, stagingTable, options)
                        }
                    }
                }
        }
    }

    protected def mergeCondition : Column = {
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

    /**
      * Removes one or more partitions.
     *
     * @param execution
      * @param partitions
      */
    override def truncate(execution: Execution, partitions: Map[String, FieldValue]): Unit = {
        require(execution != null)
        require(partitions != null)

        if (partitions.isEmpty) {
            logger.info(s"Cleaning JDBC relation '$identifier', this will truncate JDBC table $tableIdentifier")
            withConnection { (con, options) =>
                JdbcUtils.truncateTable(con, tableIdentifier, options)
            }
        }
        else {
            logger.info(s"Cleaning partitions of JDBC relation '$identifier', this will partially truncate JDBC table $tableIdentifier")
            withStatement { (statement, options) =>
                val condition = partitionCondition(partitions, options)
                JdbcUtils.truncatePartition(statement, tableIdentifier, condition, options)
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

        withConnection { (con, options) =>
            JdbcUtils.tableExists(con, tableIdentifier, options)
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
        withConnection { (con, options) =>
            if (JdbcUtils.tableExists(con, tableIdentifier, options)) {
                tableDefinition match {
                    case Some(targetTable) =>
                        val currentTable = JdbcUtils.getTableOrView(con, tableIdentifier, options)
                        val requiresChange = TableChange.requiresMigration(currentTable, targetTable, migrationPolicy)
                        val wrongType = currentTable.tableType != TableType.UNKNOWN && currentTable.tableType != targetTable.tableType
                        !requiresChange && !wrongType
                    case None => true
                }
            }
            else {
                false
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
            val condition = partitionCondition(partition, options)

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

        withConnection{ (con,options) =>
            if (!ifNotExists || !JdbcUtils.tableExists(con, tableIdentifier, options)) {
                doCreate(con, options)
                provides.foreach(execution.refreshResource)
            }
        }
    }

    protected def doCreate(con:java.sql.Connection, options:JDBCOptions): Unit = {
        val pk = tableDefinition.filter(_.primaryKey.nonEmpty).map(t => s"\n  Primary key ${t.primaryKey.mkString(",")}").getOrElse("")
        val idx = tableDefinition.map(t => t.indexes.map(i => s"\n  Index '${i.name}' on ${i.columns.mkString(",")}").foldLeft("")(_ + _)).getOrElse("")
        logger.info(s"Creating JDBC relation '$identifier', this will create JDBC table $tableIdentifier with schema\n${schema.map(_.treeString).orNull}$pk$idx")

        tableDefinition match {
            case Some(table) =>
                JdbcUtils.createTable(con, table, options)
            case None =>
                throw new UnspecifiedSchemaException(identifier)
        }
    }

    /**
      * This method will physically destroy the corresponding relation in the target JDBC database.
      * @param execution
      */
    override def destroy(execution:Execution, ifExists:Boolean=false) : Unit = {
        dropTableOrView(execution, table, ifExists)
    }

    override def migrate(execution:Execution, migrationPolicy:MigrationPolicy, migrationStrategy:MigrationStrategy) : Unit = {
        // Only try migration if schema is explicitly specified
        tableDefinition.foreach { targetTable =>
            withConnection { (con, options) =>
                if (JdbcUtils.tableExists(con, tableIdentifier, options)) {
                    val currentTable = JdbcUtils.getTableOrView(con, tableIdentifier, options)

                    // Check if table type changes
                    if (currentTable.tableType != TableType.UNKNOWN && currentTable.tableType != targetTable.tableType) {
                        // Drop view, recreate table
                        migrateFromView(migrationStrategy)
                    }
                    else if (TableChange.requiresMigration(currentTable, targetTable, migrationPolicy)) {
                        migrateFromTable(currentTable, targetTable, migrationPolicy, migrationStrategy)
                        provides.foreach(execution.refreshResource)
                    }
                }
            }
        }
    }

    private def migrateFromView(migrationStrategy:MigrationStrategy) : Unit = {
        migrationStrategy match {
            case MigrationStrategy.NEVER =>
                logger.warn(s"Migration required for JdbcTable relation '$identifier' from VIEW to a TABLE $table, but migrations are disabled.")
            case MigrationStrategy.FAIL =>
                logger.error(s"Cannot migrate JdbcTable relation '$identifier' from VIEW to a TABLE $table, since migrations are disabled.")
                throw new MigrationFailedException(identifier)
            case MigrationStrategy.ALTER|MigrationStrategy.ALTER_REPLACE|MigrationStrategy.REPLACE =>
                logger.info(s"Migrating JdbcTable relation '$identifier' from VIEW to TABLE $table")
                withConnection { (con, options) =>
                    try {
                        withStatement(con, options) { (stmt,options) =>
                            JdbcUtils.dropView(stmt, tableIdentifier, options)
                        }
                        doCreate(con, options)
                    }
                    catch {
                        case NonFatal(ex) => throw new MigrationFailedException(identifier, ex)
                    }
                }
        }
    }

    private def migrateFromTable(currentTable:TableDefinition, targetTable:TableDefinition, migrationPolicy:MigrationPolicy, migrationStrategy:MigrationStrategy) : Unit = {
        withConnection { (con, options) =>
            migrationStrategy match {
                case MigrationStrategy.NEVER =>
                    logger.warn(s"Migration required for relation '$identifier', but migrations are disabled.\nCurrent schema:\n${currentTable.schema.treeString}New schema:\n${targetTable.schema.treeString}")
                case MigrationStrategy.FAIL =>
                    logger.error(s"Cannot migrate relation '$identifier', but migrations are disabled.\nCurrent schema:\n${currentTable.schema.treeString}New schema:\n${targetTable.schema.treeString}")
                    throw new MigrationFailedException(identifier)
                case MigrationStrategy.ALTER =>
                    val dialect = SqlDialects.get(options.url)
                    val migrations = TableChange.migrate(currentTable, targetTable, migrationPolicy)
                    if (migrations.exists(m => !dialect.supportsChange(tableIdentifier, m))) {
                        logger.error(s"Cannot migrate relation JDBC relation '$identifier' of table $tableIdentifier, since that would require unsupported changes.\nCurrent schema:\n${currentTable.schema.treeString}New schema:\n${targetTable.schema.treeString}")
                        throw new MigrationFailedException(identifier)
                    }
                    alter(migrations, con, options)
                case MigrationStrategy.ALTER_REPLACE =>
                    val dialect = SqlDialects.get(options.url)
                    val migrations = TableChange.migrate(currentTable, targetTable, migrationPolicy)
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
            logger.info(s"Migrating JDBC relation '$identifier', this will alter JDBC table $tableIdentifier. New schema:\n${targetTable.schema.treeString}")
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
                logger.info(s"Migrating JDBC relation '$identifier', this will recreate JDBC table $tableIdentifier. New schema:\n${targetTable.schema.treeString}")
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
            Some(JdbcUtils.getTableSchema(con, tableIdentifier, options).catalogType)
        }
    }

    private def checkPartition(partition:Map[String,SingleValue]) : Boolean = {
        withConnection{ (connection, options) =>
            val condition = {
                if (partition.isEmpty) {
                    "1=1"
                }
                else {
                    partitionCondition(partition, options)
                }
            }
            !JdbcUtils.emptyResult(connection, tableIdentifier, condition, options)
        }
    }

    private def partitionCondition(partitions: Map[String, FieldValue], options: JDBCOptions) : String = {
        val dialect = SqlDialects.get(options.url)
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


case class JdbcTableRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema] = None,
    override val partitions: Seq[PartitionField] = Seq.empty,
    connection: Reference[Connection],
    table: TableIdentifier,
    properties: Map[String,String] = Map.empty,
    stagingTable: Option[TableIdentifier] = None,
    mergeKey: Seq[String] = Seq.empty,
    override val primaryKey: Seq[String] = Seq.empty,
    indexes: Seq[TableIndex] = Seq.empty
) extends JdbcTableRelationBase(
    instanceProperties,
    schema,
    partitions,
    connection,
    table,
    properties,
    mergeKey,
    primaryKey,
    indexes
) {
    override protected val stagingIdentifier: Option[TableIdentifier] = stagingTable
}


class JdbcTableRelationSpec extends RelationSpec with PartitionedRelationSpec with SchemaRelationSpec with IndexedRelationSpec {
    @JsonProperty(value = "connection", required = true) private var connection: ConnectionReferenceSpec = _
    @JsonProperty(value = "properties", required = false) private var properties: Map[String, String] = Map.empty
    @JsonProperty(value = "database", required = false) private var database: Option[String] = None
    @JsonProperty(value = "table", required = false) private var table: String = ""
    @JsonProperty(value = "stagingTable", required = false) private var stagingTable: Option[String] = None
    @JsonProperty(value = "mergeKey", required = false) private var mergeKey: Seq[String] = Seq.empty
    @JsonProperty(value = "primaryKey", required = false) private var primaryKey: Seq[String] = Seq.empty

    /**
      * Creates the instance of the specified Relation with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, props:Option[Relation.Properties] = None): JdbcTableRelation = {
        JdbcTableRelation(
            instanceProperties(context, props),
            schema.map(_.instantiate(context)),
            partitions.map(_.instantiate(context)),
            connection.instantiate(context),
            TableIdentifier(context.evaluate(table), context.evaluate(database)),
            context.evaluate(properties),
            context.evaluate(stagingTable).map(t => TableIdentifier(t, context.evaluate(database))),
            mergeKey.map(context.evaluate),
            primaryKey.map(context.evaluate),
            indexes.map(_.instantiate(context))
        )
    }
}
