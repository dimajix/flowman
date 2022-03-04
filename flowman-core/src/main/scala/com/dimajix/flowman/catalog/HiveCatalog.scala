/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.catalog

import java.util.Locale

import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkShim
import org.apache.spark.sql.catalyst.analysis.DatabaseAlreadyExistsException
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.execution.command.AlterTableAddColumnsCommand
import org.apache.spark.sql.execution.command.AlterTableAddPartitionCommand
import org.apache.spark.sql.execution.command.AlterTableChangeColumnCommand
import org.apache.spark.sql.execution.command.AlterTableDropPartitionCommand
import org.apache.spark.sql.execution.command.AlterTableSetLocationCommand
import org.apache.spark.sql.execution.command.AnalyzePartitionCommand
import org.apache.spark.sql.execution.command.AnalyzeTableCommand
import org.apache.spark.sql.execution.command.CreateDatabaseCommand
import org.apache.spark.sql.execution.command.CreateTableCommand
import org.apache.spark.sql.execution.command.DropDatabaseCommand
import org.apache.spark.sql.execution.command.DropTableCommand
import org.apache.spark.sql.hive.HiveClientShim
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.catalog.HiveCatalog.cleanupField
import com.dimajix.flowman.catalog.HiveCatalog.cleanupFields
import com.dimajix.flowman.catalog.TableChange.AddColumn
import com.dimajix.flowman.catalog.TableChange.UpdateColumnComment
import com.dimajix.flowman.catalog.TableChange.UpdateColumnNullability
import com.dimajix.flowman.config.Configuration
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.hadoop.FileUtils
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.PartitionSchema
import com.dimajix.spark.features.hiveVarcharSupported
import com.dimajix.spark.sql.SchemaUtils
import com.dimajix.spark.sql.SchemaUtils.replaceCharVarchar


object HiveCatalog {
    def cleanupSchema(schema:StructType) : StructType = {
        if (hiveVarcharSupported)
            schema
        else
            replaceCharVarchar(schema)
    }
    def cleanupFields(schema:Seq[StructField]) : Seq[StructField] = {
        if (hiveVarcharSupported)
            schema
        else
            schema.map(replaceCharVarchar)
    }
    def cleanupField(field:StructField) : StructField = {
        if (hiveVarcharSupported)
            field
        else
            replaceCharVarchar(field)
    }
}

/**
 * The HiveCatalog is a wrapper around the Spark external catalog, which mainly contains Hive tables.
 * @param spark
 * @param config
 * @param externalCatalogs
 */
final class HiveCatalog(val spark:SparkSession, val config:Configuration, val externalCatalogs: Seq[ExternalCatalog] = Seq()) {
    private val logger = LoggerFactory.getLogger(classOf[HiveCatalog])
    private val catalog = spark.sessionState.catalog
    private val hadoopConf = spark.sparkContext.hadoopConfiguration

    def currentDatabase : String = catalog.getCurrentDatabase

    /**
      * Lists all (external) databaes
      * @return
      */
    def listDatabases() : Seq[String] = {
        catalog.externalCatalog.listDatabases()
    }

    /**
      * Creates a new database
      */
    @throws[DatabaseAlreadyExistsException]
    def createDatabase(database:String, ignoreIfExists:Boolean) : Unit = {
        require(database != null && database.nonEmpty)

        val exists = databaseExists(database)
        if (!ignoreIfExists && exists) {
            throw new DatabaseAlreadyExistsException(database)
        }

        if (!exists) {
            logger.info(s"Creating Hive database $database")
            val cmd = CreateDatabaseCommand(database, ignoreIfExists, None, None, Map())
            cmd.run(spark)
        }
    }

    /**
      * Returns true if the specified Hive database actually exists
      * @param database
      * @return
      */
    def databaseExists(database:String) : Boolean = {
        require(database != null)
        val dbName = formatDatabaseName(database)
        catalog.externalCatalog.databaseExists(dbName)
    }

    /**
      * Creates a new database
      */
    @throws[NoSuchDatabaseException]
    def dropDatabase(database:String, ignoreIfNotExists:Boolean) : Unit = {
        require(database != null && database.nonEmpty)

        val exists = databaseExists(database)
        if (ignoreIfNotExists && !exists) {
            throw new NoSuchDatabaseException(database)
        }

        if (exists) {
            logger.info(s"Dropping Hive database $database")
            val cmd = DropDatabaseCommand(database, ignoreIfNotExists, true)
            cmd.run(spark)
        }
    }

    /**
      * Lists all tables in an external database
      * @return
      */
    @throws[NoSuchDatabaseException]
    def listTables(database:String) : Seq[TableIdentifier] = {
        // Directly query external catalog, so we don't get temp tables
        val dbName = formatDatabaseName(database)
        catalog.externalCatalog
            .listTables(dbName)
            .map(name =>TableIdentifier(name, Seq(database)))
    }

    /**
      * Lists all tables matching a specific name pattern in an external database
      * @return
      */
    @throws[NoSuchDatabaseException]
    def listTables(database:String, pattern:String) : Seq[TableIdentifier] = {
        // Directly query external catalog, so we don't get temp tables
        val dbName = formatDatabaseName(database)
        catalog.externalCatalog
            .listTables(dbName, pattern)
            .map(name =>TableIdentifier(name, Seq(database)))
    }

    /**
      * Creates a new table from a detailed definition
      * @param table
      * @param ignoreIfExists
      */
    @throws[NoSuchDatabaseException]
    @throws[TableAlreadyExistsException]
    def createTable(table:CatalogTable, ignoreIfExists:Boolean) : Unit = {
        require(table != null)

        val exists = tableExists(TableIdentifier.of(table))
        if (!ignoreIfExists && exists) {
            throw new TableAlreadyExistsException(table.identifier.database.getOrElse(""), table.identifier.table)
        }

        if (!exists) {
            // Cleanup table definition
            val cleanedSchema = SchemaUtils.truncateComments(table.schema, maxCommentLength)
            val catalogSchema = HiveCatalog.cleanupSchema(cleanedSchema)
            val cleanedTable = table.copy(schema = catalogSchema)

            logger.info(s"Creating Hive table ${table.identifier}")
            val cmd = CreateTableCommand(cleanedTable, ignoreIfExists)
            cmd.run(spark)

            // Create directory if not exists
            table.storage.locationUri.foreach { uri =>
                val location = new Path(uri)
                val fs = location.getFileSystem(hadoopConf)
                FileUtils.createLocation(fs, location)
            }

            // Publish table to external catalog
            externalCatalogs.foreach(_.createTable(table))
        }
    }

    /**
      * Refreshes any meta information about a table. This might be required either when the schema changes
      * or when new data is written into a table
      * @param table
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def refreshTable(table:TableIdentifier) : Unit = {
        require(table != null)

        if (!tableExists(table)) {
            throw new NoSuchTableException(table.database.getOrElse(""), table.table)
        }

        if (config.flowmanConf.hiveAnalyzeTable) {
            val cmd = AnalyzeTableCommand(table.toSpark, false)
            cmd.run(spark)
        }

        // Publish table to external catalog
        externalCatalogs.foreach { catalog =>
            val definition = getTable(table)
            catalog.refreshTable(definition)
        }
    }

    /**
      * Returns true if the specified Hive table or view actually exists
      * @param name
      * @return
      */
    def tableExists(name:TableIdentifier) : Boolean = {
        catalog.tableExists(name.toSpark)
    }

    /**
      * Returns information about a Hive table or view. Note that this call will return the catalog information itself,
      * which may contain data types not directly supported by Spark (VARCHAR, CHAR)
      * @param name
      * @return
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def getTable(name:TableIdentifier) : CatalogTable = {
        val db = formatDatabaseName(name.database.getOrElse(catalog.getCurrentDatabase))
        val table = formatTableName(name.table)
        requireDbExists(db)
        requireTableExists(TableIdentifier(table, Seq(db)))
        catalog.externalCatalog.getTable(db, table)
    }

    /**
      * Returns the storage location of a Hive table
      * @param name
      * @return
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def getTableLocation(name:TableIdentifier) : Path = {
        val meta = getTable(name)
        require(meta.tableType != CatalogTableType.VIEW)
        new Path(meta.location)
    }

    /**
      * Drops a whole table including all partitions and all files
      * @param table
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def dropTable(table:TableIdentifier, ignoreIfNotExists:Boolean=false) : Unit = {
        require(table != null)

        val db = formatDatabaseName(table.database.getOrElse(catalog.getCurrentDatabase))
        val exists = tableExists(table)
        if (!ignoreIfNotExists && !exists) {
            throw new NoSuchTableException(db, table.table)
        }

        if (exists) {
            logger.info(s"Dropping Hive table/view $table")
            val catalogTable =  catalog.externalCatalog.getTable(db, formatTableName(table.table))
            require(catalogTable.tableType != CatalogTableType.VIEW)

            // Delete all partitions
            if (catalogTable.partitionSchema != null && catalogTable.partitionSchema.fields.nonEmpty) {
                catalog.listPartitions(table.toSpark).foreach { p =>
                    val location = new Path(p.location)
                    val fs = location.getFileSystem(hadoopConf)
                    FileUtils.deleteLocation(fs, location)
                }
            }

            // Delete table itself
            val cmd = DropTableCommand(table.toSpark, ignoreIfNotExists, false, true)
            cmd.run(spark)

            // Delete location to cleanup any remaining files
            val location = new Path(catalogTable.location)
            val fs = location.getFileSystem(hadoopConf)
            FileUtils.deleteLocation(fs, location)

            // Remove table from external catalog
            externalCatalogs.foreach(_.dropTable(catalogTable))
        }
    }

    /**
      * Truncates a table by either removing the corresponding file or by dropping all partitions
      * @param table
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def truncateTable(table:TableIdentifier) : Unit = {
        require(table != null)
        logger.info(s"Truncating Hive table $table")

        val catalogTable = getTable(table)
        require(catalogTable.tableType != CatalogTableType.VIEW)

        // First drop partitions
        if (catalogTable.partitionSchema != null && catalogTable.partitionSchema.fields.nonEmpty) {
            dropPartitions(table, catalog.listPartitions(table.toSpark).map(p => PartitionSpec(p.spec)))
        }

        // Then cleanup directory from any remainders
        val location = new Path(catalogTable.location)
        val fs = location.getFileSystem(hadoopConf)
        FileUtils.truncateLocation(fs, location)

        spark.catalog.refreshTable(table.quotedString)
        externalCatalogs.foreach(_.truncateTable(catalogTable))
    }

    /**
     * Applies a list of [[TableChange]] modifications to an existing Hive table. If a certain change is not
     * supported, then an [[UnsupportedOperationException]] will be thrown and no change will be applied at all.
     * @param table
     * @param changes
     */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def alterTable(table:TableIdentifier, changes:Seq[TableChange]) : Unit = {
        require(table != null)
        require(changes != null)

        val catalogTable = getTable(table)
        require(catalogTable.tableType != CatalogTableType.VIEW)
        val tableColumns = catalogTable.schema.fields.map(f => (f.name.toLowerCase(Locale.ROOT), f)).toMap

        val colsToAdd = mutable.Buffer[StructField]()
        changes.foreach {
            case a:AddColumn =>
                logger.info(s"Adding column ${a.column.name} with type ${a.column.catalogType.sql} to Hive table '$table'")
                colsToAdd.append(cleanupField(a.column.catalogField))
            case u:UpdateColumnNullability =>
                logger.info(s"Updating nullability of column ${u.column} to ${u.nullable} in Hive table '$table'")
                val field = tableColumns.getOrElse(u.column.toLowerCase(Locale.ROOT), throw new IllegalArgumentException(s"Table column ${u.column} does not exist in table $table"))
                    .copy(nullable = u.nullable)
                val cmd = AlterTableChangeColumnCommand(table.toSpark, u.column, cleanupField(field))
                cmd.run(spark)
            case u:UpdateColumnComment =>
                logger.info(s"Updating comment of column ${u.column} in Hive table '$table'")
                val field = tableColumns.getOrElse(u.column.toLowerCase(Locale.ROOT), throw new IllegalArgumentException(s"Table column ${u.column} does not exist in table $table"))
                    .withComment(u.comment.getOrElse(""))
                val cmd = AlterTableChangeColumnCommand(table.toSpark, u.column, cleanupField(field))
                cmd.run(spark)
            case x:TableChange => throw new UnsupportedOperationException(s"Unsupported table change $x for Hive table $table")
        }

        if (colsToAdd.nonEmpty) {
            val cmd = AlterTableAddColumnsCommand(table.toSpark, colsToAdd)
            cmd.run(spark)
        }

        externalCatalogs.foreach(_.alterTable(catalogTable))
    }

    /**
     * Adds one or more new columns to an existing Hive table.
     * @param table
     * @param colsToAdd
     */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def addTableColumns(table:TableIdentifier, colsToAdd: Seq[StructField]) : Unit = {
        require(table != null)
        require(colsToAdd != null)
        logger.info(s"Adding new columns ${colsToAdd.map(_.name).mkString(",")} to existing Hive table '$table'")

        val catalogTable = getTable(table)
        require(catalogTable.tableType != CatalogTableType.VIEW)

        val cleanedCols = cleanupFields(colsToAdd)
        val cmd = AlterTableAddColumnsCommand(table.toSpark, cleanedCols)
        cmd.run(spark)

        externalCatalogs.foreach(_.alterTable(catalogTable))
    }

    /**
      * Checks if the given partition exists. Will throw an exception if the corresponding table does not exist
      * @param table
      * @param partition
      * @return
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def partitionExists(table:TableIdentifier, partition:PartitionSpec) : Boolean = {
        require(table != null)
        require(partition != null)
        catalog.listPartitions(table.toSpark, Some(partition.mapValues(_.toString).toMap).filter(_.nonEmpty)).nonEmpty
    }

    /**
     * Retrieve the metadata of a table partition, assuming it exists.
     * If no database is specified, assume the table is in the current database.
     */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    @throws[NoSuchPartitionException]
    def getPartition(table: TableIdentifier, partition:PartitionSpec): CatalogTablePartition = {
        catalog.getPartition(table.toSpark, partition.mapValues(_.toString).toMap)
    }

    /**
      * Returns the storage location of a single partition of a Hive table
      * @param table
      * @param partition
      * @return
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    @throws[NoSuchPartitionException]
    def getPartitionLocation(table:TableIdentifier, partition:PartitionSpec) : Path = {
        require(table != null)
        require(partition != null && partition.nonEmpty)
        new Path(getPartition(table, partition).location)
    }

    /**
      * Returns the partition schema of a Hive table. If the table is not partitioned, an empty schema will be returned
      * @param table
      * @return
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def getPartitionSchema(table:TableIdentifier) : PartitionSchema = {
        require(table != null)
        val schema = getTable(table).partitionSchema
        PartitionSchema(schema.fields.map(field => PartitionField.fromSpark(field)))
    }

    /**
      * Adds a new partition to an existing table. Will throw an exception if the partition already exists
      * @param table
      * @param partition
      * @param location
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def addPartition(table:TableIdentifier, partition:PartitionSpec, location:Path) : Unit = {
        require(table != null)
        require(partition != null && partition.nonEmpty)
        require(location != null && location.toString.nonEmpty)

        logger.info(s"Adding partition ${partition.spec} to table $table at '$location'")
        val sparkPartition = partition.mapValues(_.toString).toMap
        val cmd = AlterTableAddPartitionCommand(table.toSpark, Seq((sparkPartition, Some(location.toString))), false)
        cmd.run(spark)

        analyzePartition(table, sparkPartition)

        externalCatalogs.foreach { ec =>
            val catalogTable = getTable(table)
            val catalogPartition = catalog.getPartition(table.toSpark, sparkPartition)
            ec.addPartition(catalogTable, catalogPartition)
        }
    }

    /**
      * Adds a new partition or replaces an existing partition to an existing table.
      * @param table
      * @param partition
      * @param location
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def addOrReplacePartition(table:TableIdentifier, partition:PartitionSpec, location:Path) : Unit = {
        require(table != null)
        require(partition != null && partition.nonEmpty)
        require(location != null && location.toString.nonEmpty)

        val sparkPartition = partition.mapValues(_.toString).toMap
        if (partitionExists(table, partition)) {
            logger.info(s"Replacing partition ${partition.spec} of table $table with location '$location'")
            val cmd = AlterTableSetLocationCommand(table.toSpark, Some(sparkPartition), location.toString)
            cmd.run(spark)

            refreshPartition(table, partition)
        }
        else {
            addPartition(table, partition, location)
        }
    }

    /**
      * Adds a new partition or replaces an existing partition to an existing table.
      * @param table
      * @param partition
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    @throws[NoSuchPartitionException]
    def refreshPartition(table:TableIdentifier, partition:PartitionSpec) : Unit = {
        require(table != null)
        require(partition != null && partition.nonEmpty)

        val sparkPartition = partition.mapValues(_.toString).toMap
        logger.info(s"Refreshing partition ${partition.spec} of table $table")

        if (!partitionExists(table, partition)) {
            throw new NoSuchPartitionException(table.database.getOrElse(catalog.getCurrentDatabase), table.table, sparkPartition)
        }

        analyzePartition(table, sparkPartition)

        externalCatalogs.foreach { ec =>
            val catalogTable = getTable(table)
            val catalogPartition = catalog.getPartition(table.toSpark, sparkPartition)
            ec.alterPartition(catalogTable, catalogPartition)
        }
    }

    private def analyzePartition(table:TableIdentifier, sparkPartition:Map[String,String]) : Unit = {
        def doIt(): Unit = {
            val cmd = AnalyzePartitionCommand(table.toSpark, sparkPartition.map { case (k, v) => k -> Some(v) }, false)
            cmd.run(spark)
        }

        if (config.flowmanConf.hiveAnalyzeTable) {
            if (config.flowmanConf.getConf(FlowmanConf.WORKAROUND_ANALYZE_PARTITION)) {
                // This is a workaround for CDP 7.1, where Hive State is not set in AnalyzePartitions
                HiveClientShim.withHiveSession(spark) {
                    doIt()
                }
            }
            else {
                doIt()
            }
        }
    }

    /**
      * Truncates a single partition by removing all files (but keeping the directory)
      * @param table
      * @param partition
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    @throws[NoSuchPartitionException]
    def truncatePartition(table:TableIdentifier, partition:PartitionSpec) : Unit = {
        require(table != null)
        require(partition != null && partition.nonEmpty)

        logger.info(s"Truncating partition ${partition.spec} of Hive table $table")
        val location = getPartitionLocation(table, partition)
        val fs = location.getFileSystem(hadoopConf)
        FileUtils.truncateLocation(fs, location)

        externalCatalogs.foreach { ec =>
            val sparkPartition = partition.mapValues(_.toString).toMap
            val catalogTable = getTable(table)
            val catalogPartition = catalog.getPartition(table.toSpark, sparkPartition)
            ec.truncatePartition(catalogTable, catalogPartition)
        }
    }

    /**
      * Drops a Hive table partition and removes the corresponding directory or file
      * @param table
      * @param partition
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    @throws[NoSuchPartitionException]
    def dropPartition(table:TableIdentifier, partition:PartitionSpec, ignoreIfNotExists:Boolean=false) : Unit = {
        require(table != null)
        require(partition != null && partition.nonEmpty)
        dropPartitions(table, Seq(partition), ignoreIfNotExists)
    }

    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    @throws[NoSuchPartitionException]
    def dropPartitions(table:TableIdentifier, partitions:Seq[PartitionSpec], ignoreIfNotExists:Boolean=false) : Unit = {
        require(table != null)
        require(partitions != null)

        // Check which partitions actually exist
        val flaggedPartitions = partitions.map(p => (p, partitionExists(table, p)))
        if (!ignoreIfNotExists && flaggedPartitions.exists(!_._2)) {
            val missingPartitions = flaggedPartitions.filter(!_._2).head
            val oneMissingPartition = missingPartitions._1.mapValues(_.toString)
            throw new NoSuchPartitionException(table.database.getOrElse(""), table.table, oneMissingPartition.toMap)
        }

        // Keep only those partitions which really need to be dropped
        val dropPartitions = flaggedPartitions.filter(_._2).map(_._1)
        // Convert to Spark partitions
        val sparkPartitions = dropPartitions.map(_.mapValues(_.toString).toMap)
        // Convert to external catalog partitions which can be reused in the last step
        val catalogPartitions = sparkPartitions.map(catalog.getPartition(table.toSpark, _)).filter(_ != null)

        logger.info(s"Dropping partitions ${dropPartitions.map(_.spec).mkString(",")} from Hive table $table")
        catalogPartitions.foreach { partition =>
            val location = new Path(partition.location)
            val fs = location.getFileSystem(hadoopConf)
            FileUtils.deleteLocation(fs, location)
        }

        // Note that "purge" is not supported with Hive < 1.2
        val cmd = AlterTableDropPartitionCommand(table.toSpark, sparkPartitions, ignoreIfNotExists, purge = false, retainData = false)
        cmd.run(spark)

        externalCatalogs.foreach { ec =>
            val catalogTable = getTable(table)
            catalogPartitions.foreach(ec.dropPartition(catalogTable, _))
        }
    }

    @throws[NoSuchDatabaseException]
    @throws[TableAlreadyExistsException]
    def createView(table:TableIdentifier, select:String, ignoreIfExists:Boolean): Unit = {
        require(table != null)

        val exists = tableExists(table)
        if (!ignoreIfExists && exists) {
            throw new TableAlreadyExistsException(table.database.getOrElse(""), table.table)
        }

        if (!exists) {
            logger.info(s"Creating Hive view $table")

            val plan = spark.sql(select).queryExecution.analyzed
            val cmd = SparkShim.createView(table.toSpark, select, plan, false, false)
            cmd.run(spark)

            // Publish view to external catalog
            externalCatalogs.foreach(_.createView(getTable(table)))
        }
    }

    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def alterView(table:TableIdentifier, select:String): Unit = {
        require(table != null)

        logger.info(s"Redefining Hive view $table")

        val plan = spark.sql(select).queryExecution.analyzed
        val cmd = SparkShim.alterView(table.toSpark, select, plan)
        cmd.run(spark)

        // Publish view to external catalog
        externalCatalogs.foreach(_.alterView(getTable(table)))
    }

    /**
      * Drops a whole table including all partitions and all files
      * @param table
      */
    @throws[NoSuchDatabaseException]
    @throws[NoSuchTableException]
    def dropView(table:TableIdentifier, ignoreIfNotExists:Boolean=false) : Unit = {
        require(table != null)

        val exists = tableExists(table)
        if (!ignoreIfNotExists && !exists) {
            throw new NoSuchTableException(table.database.getOrElse(catalog.getCurrentDatabase), table.table)
        }

        if (exists) {
            logger.info(s"Dropping Hive view $table")
            val catalogTable = getTable(table)
            require(catalogTable.tableType == CatalogTableType.VIEW)

            // Delete table itself
            val cmd = DropTableCommand(table.toSpark, ignoreIfNotExists, true, false)
            cmd.run(spark)

            // Remove table from external catalog
            externalCatalogs.foreach(_.dropView(catalogTable))
        }
    }

    private def maxCommentLength : Int = {
        if (spark.conf.getOption("spark.hadoop.hive.metastore.uris").isEmpty
            || spark.conf.getOption("javax.jdo.option.ConnectionURL").exists(_.contains("jdbc:derby:"))
            || spark.conf.getOption("spark.hadoop.javax.jdo.option.ConnectionURL").exists(_.contains("jdbc:derby:"))) {
            254
        }
        else {
            4000
        }
    }


    /**
     * Format table name, taking into account case sensitivity.
     */
    private def formatTableName(name: String): String = {
        val caseSensitive = false // catalog.conf.caseSensitiveAnalysis
        if (caseSensitive) name else name.toLowerCase(Locale.ROOT)
    }

    /**
     * Format database name, taking into account case sensitivity.
     */
    private def formatDatabaseName(name: String): String = {
        val caseSensitive = false // catalog.conf.caseSensitiveAnalysis
        if (caseSensitive) name else name.toLowerCase(Locale.ROOT)
    }

    private def requireDbExists(db: String): Unit = {
        if (!databaseExists(db)) {
            throw new NoSuchDatabaseException(db)
        }
    }

    private def requireTableExists(name: TableIdentifier): Unit = {
        if (!tableExists(name)) {
            val db = name.database.getOrElse(catalog.getCurrentDatabase)
            throw new NoSuchTableException(db = db, table = name.table)
        }
    }

    private def requireTableNotExists(name: TableIdentifier): Unit = {
        if (tableExists(name)) {
            val db = name.database.getOrElse(catalog.getCurrentDatabase)
            throw new TableAlreadyExistsException(db = db, table = name.table)
        }
    }
}
