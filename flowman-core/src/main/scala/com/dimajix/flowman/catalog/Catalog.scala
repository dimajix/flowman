/*
 * Copyright 2018 Kaya Kupferschmidt
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

import java.io.FileNotFoundException

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkShim
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.DatabaseAlreadyExistsException
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.execution.command.AlterTableAddColumnsCommand
import org.apache.spark.sql.execution.command.AlterTableAddPartitionCommand
import org.apache.spark.sql.execution.command.AlterTableDropPartitionCommand
import org.apache.spark.sql.execution.command.AlterTableSetLocationCommand
import org.apache.spark.sql.execution.command.AlterViewAsCommand
import org.apache.spark.sql.execution.command.AnalyzePartitionCommand
import org.apache.spark.sql.execution.command.AnalyzeTableCommand
import org.apache.spark.sql.execution.command.CreateDatabaseCommand
import org.apache.spark.sql.execution.command.CreateTableCommand
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.execution.command.DropDatabaseCommand
import org.apache.spark.sql.execution.command.DropTableCommand
import org.apache.spark.sql.types.StructField
import org.slf4j.LoggerFactory
import com.dimajix.flowman.config.Configuration
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.PartitionSchema
import com.dimajix.flowman.util.SchemaUtils


class Catalog(val spark:SparkSession, val config:Configuration, val externalCatalogs: Seq[ExternalCatalog] = Seq()) {
    private val logger = LoggerFactory.getLogger(classOf[Catalog])
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
        // "SHOW TABLES IN training LIKE 'weather_raw'"
        catalog.databaseExists(database)
    }

    /**
      * Creates a new database
      */
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
    def listTables(database:String) : Seq[TableIdentifier] = {
        catalog.externalCatalog
            .listTables(database)
            .map(name =>TableIdentifier(name, Some(database)))
    }

    /**
      * Lists all tables matching a specific name pattern in an external database
      * @return
      */
    def listTables(database:String, pattern:String) : Seq[TableIdentifier] = {
        catalog.externalCatalog
            .listTables(database, pattern)
            .map(name =>TableIdentifier(name, Some(database)))
    }

    /**
      * Creates a new table from a detailed definition
      * @param table
      * @param ignoreIfExists
      */
    def createTable(table:CatalogTable, ignoreIfExists:Boolean) : Unit = {
        require(table != null)

        val exists = tableExists(table.identifier)
        if (!ignoreIfExists && exists) {
            throw new TableAlreadyExistsException(table.identifier.database.getOrElse(""), table.identifier.table)
        }

        if (!exists) {
            // Cleanup table definition
            val cleanedSchema = SchemaUtils.truncateComments(table.schema, maxCommentLength)
            val cleanedTable = table.copy(schema = cleanedSchema)

            logger.info(s"Creating Hive table ${table.identifier}")
            val cmd = CreateTableCommand(cleanedTable, ignoreIfExists)
            cmd.run(spark)

            // Create directory if not exists
            table.storage.locationUri.foreach(location => createLocation(new Path(location)))

            // Publish table to external catalog
            externalCatalogs.foreach(_.createTable(table))
        }
    }

    /**
      * Refreshes any meta information about a table. This might be required either when the schema changes
      * or when new data is written into a table
      * @param table
      */
    def refreshTable(table:TableIdentifier) : Unit = {
        require(table != null)

        if (!tableExists(table)) {
            throw new TableAlreadyExistsException(table.database.getOrElse(""), table.table)
        }

        if (config.flowmanConf.hiveAnalyzeTable) {
            val cmd = AnalyzeTableCommand(table, false)
            cmd.run(spark)
        }

        // Publish table to external catalog
        externalCatalogs.foreach { catalog =>
            val definition = this.catalog.externalCatalog.getTable(table.database.getOrElse(""), table.table)
            catalog.alterTable(definition)
        }
    }

    /**
      * Returns true if the specified Hive table or view actually exists
      * @param table
      * @return
      */
    def tableExists(table:TableIdentifier) : Boolean = {
        require(table != null)

        catalog.tableExists(table)
    }

    /**
      * Returns information about a Hive table or view
      * @param table
      * @return
      */
    def getTable(table:TableIdentifier) : CatalogTable = {
        require(table != null)

        catalog.getTableMetadata(table)
    }

    /**
      * Returns the storage location of a Hive table
      * @param table
      * @return
      */
    def getTableLocation(table:TableIdentifier) : Path = {
        require(table != null)
        val meta = catalog.getTableMetadata(table)
        require(meta.tableType != CatalogTableType.VIEW)
        new Path(meta.location)
    }

    /**
      * Drops a whole table including all partitions and all files
      * @param table
      */
    def dropTable(table:TableIdentifier, ignoreIfNotExists:Boolean=false) : Unit = {
        require(table != null)

        val exists = tableExists(table)
        if (!ignoreIfNotExists && !exists) {
            throw new NoSuchTableException(table.database.getOrElse(""), table.table)
        }

        if (exists) {
            logger.info(s"Dropping Hive table/view $table")
            val catalogTable = catalog.getTableMetadata(table)
            require(catalogTable.tableType != CatalogTableType.VIEW)

            // Delete all partitions
            if (catalogTable.partitionSchema != null && catalogTable.partitionSchema.fields.nonEmpty) {
                catalog.listPartitions(table).foreach(p => deleteLocation(new Path(p.location)))
            }

            val location = getTableLocation(table)
            deleteLocation(location)

            // Delete table itself
            val cmd = DropTableCommand(table, ignoreIfNotExists, false, true)
            cmd.run(spark)

            // Remove table from external catalog
            externalCatalogs.foreach(_.dropTable(catalogTable))
        }
    }

    /**
      * Truncates a table by either removing the corresponding file or by dropping all partitions
      * @param table
      */
    def truncateTable(table:TableIdentifier) : Unit = {
        require(table != null)
        logger.info(s"Truncating Hive table $table")

        val catalogTable = catalog.getTableMetadata(table)
        require(catalogTable.tableType != CatalogTableType.VIEW)

        val location = new Path(catalogTable.location)
        truncateLocation(location)

        if (catalogTable.partitionSchema != null && catalogTable.partitionSchema.fields.nonEmpty) {
            dropPartitions(table, catalog.listPartitions(table).map(p => PartitionSpec(p.parameters)))
        }

        externalCatalogs.foreach(_.truncateTable(catalogTable))
    }

    def addTableColumns(table:TableIdentifier, colsToAdd: Seq[StructField]) : Unit = {
        require(table != null)
        require(colsToAdd != null)
        logger.info(s"Adding new columns ${colsToAdd.map(_.name).mkString(",")} to existing Hive table '$table'")

        val catalogTable = catalog.getTableMetadata(table)
        require(catalogTable.tableType != CatalogTableType.VIEW)

        val cmd = AlterTableAddColumnsCommand(table, colsToAdd)
        cmd.run(spark)

        externalCatalogs.foreach(_.alterTable(catalogTable))
    }

    /**
      * Returns the storage location of a single partition of a Hive table
      * @param table
      * @param partition
      * @return
      */
    def partitionExists(table:TableIdentifier, partition:PartitionSpec) : Boolean = {
        require(table != null)
        require(partition != null)
        catalog.listPartitions(table, Some(partition.mapValues(_.toString).toMap).filter(_.nonEmpty)).nonEmpty
    }

    /**
      * Returns the storage location of a single partition of a Hive table
      * @param table
      * @param partition
      * @return
      */
    def getPartitionLocation(table:TableIdentifier, partition:PartitionSpec) : Path = {
        require(table != null)
        require(partition != null && partition.nonEmpty)
        // "DESCRIBE FORMATTED training.weather_raw PARTITION(year=2005, station='x')"
        new Path(catalog.getPartition(table, partition.mapValues(_.toString).toMap).location)
    }

    /**
      * Returns the partition schema of a Hive table. If the table is not partitioned, an empty schema will be returned
      * @param table
      * @return
      */
    def getPartitionSchema(table:TableIdentifier) : PartitionSchema = {
        require(table != null)
        // "DESCRIBE FORMATTED training.weather_raw"
        val schema = catalog.getTableMetadata(table).partitionSchema
        PartitionSchema(schema.fields.map(field => PartitionField.fromSpark(field)))
    }

    /**
      * Adds a new partition to an existing table. Will throw an exception if the partition already exists
      * @param table
      * @param partition
      * @param location
      */
    def addPartition(table:TableIdentifier, partition:PartitionSpec, location:Path) : Unit = {
        require(table != null)
        require(partition != null && partition.nonEmpty)
        require(location != null && location.toString.nonEmpty)

        logger.info(s"Adding partition ${partition.spec} to table $table at '$location'")
        val sparkPartition = partition.mapValues(_.toString).toMap
        val cmd = AlterTableAddPartitionCommand(table, Seq((sparkPartition, Some(location.toString))), false)
        cmd.run(spark)

        if (config.flowmanConf.hiveAnalyzeTable) {
            val cmd = AnalyzePartitionCommand(table, sparkPartition.map { case (k, v) => k -> Some(v) }, false)
            cmd.run(spark)
        }

        externalCatalogs.foreach { ec =>
            val catalogTable = catalog.getTableMetadata(table)
            val catalogPartition = catalog.getPartition(table, sparkPartition)
            ec.addPartition(catalogTable, catalogPartition)
        }
    }

    /**
      * Adds a new partition or replaces an existing partition to an existing table.
      * @param table
      * @param partition
      * @param location
      */
    def addOrReplacePartition(table:TableIdentifier, partition:PartitionSpec, location:Path) : Unit = {
        require(table != null)
        require(partition != null && partition.nonEmpty)
        require(location != null && location.toString.nonEmpty)

        val sparkPartition = partition.mapValues(_.toString).toMap
        if (partitionExists(table, partition)) {
            logger.info(s"Replacing partition ${partition.spec} of table $table with location '$location'")
            val cmd = AlterTableSetLocationCommand(table, Some(sparkPartition), location.toString)
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
    def refreshPartition(table:TableIdentifier, partition:PartitionSpec) : Unit = {
        require(table != null)
        require(partition != null && partition.nonEmpty)

        val sparkPartition = partition.mapValues(_.toString).toMap
        logger.info(s"Refreshing partition ${partition.spec} of table $table")

        if (!partitionExists(table, partition)) {
            throw new NoSuchPartitionException(table.database.getOrElse(""), table.table, sparkPartition)
        }

        if (config.flowmanConf.hiveAnalyzeTable) {
            val cmd = AnalyzePartitionCommand(table, sparkPartition.map { case (k, v) => k -> Some(v) }, false)
            cmd.run(spark)
        }

        externalCatalogs.foreach { ec =>
            val catalogTable = catalog.getTableMetadata(table)
            val catalogPartition = catalog.getPartition(table, sparkPartition)
            ec.alterPartition(catalogTable, catalogPartition)
        }
    }

    /**
      * Truncates a single partition by removing all files (but keeping the directory)
      * @param table
      * @param partition
      */
    def truncatePartition(table:TableIdentifier, partition:PartitionSpec) : Unit = {
        require(table != null)
        require(partition != null && partition.nonEmpty)

        logger.info(s"Truncating partition ${partition.spec} of Hive table $table")
        val location = getPartitionLocation(table, partition)
        truncateLocation(location)

        externalCatalogs.foreach { ec =>
            val sparkPartition = partition.mapValues(_.toString).toMap
            val catalogTable = catalog.getTableMetadata(table)
            val catalogPartition = catalog.getPartition(table, sparkPartition)
            ec.truncatePartition(catalogTable, catalogPartition)
        }
    }

    /**
      * Drops a Hive table partition and removes the corresponding directory or file
      * @param table
      * @param partition
      */
    def dropPartition(table:TableIdentifier, partition:PartitionSpec, ignoreIfNotExists:Boolean=false) : Unit = {
        require(table != null)
        require(partition != null && partition.nonEmpty)
        dropPartitions(table, Seq(partition), ignoreIfNotExists)
    }

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
        val catalogPartitions = sparkPartitions.map(catalog.getPartition(table, _)).filter(_ != null)

        logger.info(s"Dropping partitions ${dropPartitions.map(_.spec).mkString(",")} from Hive table $table")
        catalogPartitions.foreach { partition =>
            val location = new Path(partition.location)
            deleteLocation(location)
        }

        // Note that "purge" is not supported with Hive < 1.2
        val cmd = AlterTableDropPartitionCommand(table, sparkPartitions, ignoreIfNotExists, purge = false, retainData = false)
        cmd.run(spark)

        externalCatalogs.foreach { ec =>
            val catalogTable = catalog.getTableMetadata(table)
            catalogPartitions.foreach(ec.dropPartition(catalogTable, _))
        }
    }

    def createView(table:TableIdentifier, select:String, ignoreIfExists:Boolean): Unit = {
        require(table != null)

        val exists = tableExists(table)
        if (!ignoreIfExists && exists) {
            throw new TableAlreadyExistsException(table.database.getOrElse(""), table.table)
        }

        if (!exists) {
            logger.info(s"Creating Hive view $table")

            val plan = spark.sql(select).queryExecution.logical
            val cmd = CreateViewCommand(table, Nil, None, Map(), Some(select), plan, false, false, SparkShim.PersistedView)
            cmd.run(spark)

            // Publish view to external catalog
            externalCatalogs.foreach(_.createView(getTable(table)))
        }
    }

    def alterView(table:TableIdentifier, select:String): Unit = {
        require(table != null)

        logger.info(s"Redefining Hive view $table")

        val plan = spark.sql(select).queryExecution.logical
        val cmd = AlterViewAsCommand(table, select, plan)
        cmd.run(spark)

        // Publish view to external catalog
        externalCatalogs.foreach(_.alterView(getTable(table)))
    }

    /**
      * Drops a whole table including all partitions and all files
      * @param table
      */
    def dropView(table:TableIdentifier, ignoreIfNotExists:Boolean=false) : Unit = {
        require(table != null)

        val exists = tableExists(table)
        if (!ignoreIfNotExists && !exists) {
            throw new NoSuchTableException(table.database.getOrElse(""), table.table)
        }

        if (exists) {
            logger.info(s"Dropping Hive view $table")
            val catalogTable = catalog.getTableMetadata(table)
            require(catalogTable.tableType == CatalogTableType.VIEW)

            // Delete table itself
            val cmd = DropTableCommand(table, ignoreIfNotExists, true, false)
            cmd.run(spark)

            // Remove table from external catalog
            externalCatalogs.foreach(_.dropView(catalogTable))
        }
    }

    private def truncateLocation(location:Path): Unit = {
      val fs = location.getFileSystem(hadoopConf)
      try {
        val status = fs.getFileStatus(location)
        if (status.isDirectory()) {
            logger.info(s"Deleting all files in directory '$location'")
            java.lang.System.gc() // Release open file handles on Windows
            fs.listStatus(location).foreach { f =>
                doDelete(fs, f.getPath, true)
            }
        }
        else if (status.isFile()) {
            logger.info(s"Deleting single file '$location'")
            doDelete(fs, location, false)
        }
      } catch {
        case _:FileNotFoundException =>
      }
    }
    private def createLocation(location:Path) : Unit = {
      val fs = location.getFileSystem(hadoopConf)
      if (!fs.exists(location)) {
        logger.info(s"Creating directory '$location'")
        fs.mkdirs(location)
      }
    }
    private def deleteLocation(location:Path): Unit = {
        val fs = location.getFileSystem(hadoopConf)
        if (fs.exists(location)) {
            logger.info(s"Deleting file or directory '$location'")
            doDelete(fs, location, true)
        }
    }

    private def doDelete(fs:FileSystem, location:Path, recursive:Boolean) : Unit = {
        java.lang.System.gc() // Release open file handles on Windows
        if (!fs.delete(location, recursive)) {
            logger.warn(s"Cannot delete file or directory '$location'")
        }
    }

    private def maxCommentLength : Int = {
        if (spark.conf.getOption("spark.hadoop.hive.metastore.uris").isEmpty
            || spark.conf.getOption("javax.jdo.option.ConnectionURL").exists(_.contains("jdbc:derby:"))) {
            254
        }
        else {
            4000
        }
    }
}
