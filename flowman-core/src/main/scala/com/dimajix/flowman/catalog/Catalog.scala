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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.command.AlterTableAddPartitionCommand
import org.apache.spark.sql.execution.command.AlterTableDropPartitionCommand
import org.apache.spark.sql.execution.command.AlterTableSetLocationCommand
import org.apache.spark.sql.execution.command.CreateTableCommand
import org.apache.spark.sql.execution.command.DropTableCommand
import org.slf4j.LoggerFactory

import com.dimajix.flowman.spec.schema.PartitionField
import com.dimajix.flowman.spec.schema.PartitionSchema


class Catalog(val spark:SparkSession) {
    private val logger = LoggerFactory.getLogger(classOf[Catalog])
    private val catalog = spark.sessionState.catalog
    private val hadoopConf = spark.sparkContext.hadoopConfiguration

    /**
      * Creates a new table from a detailed definition
      * @param table
      * @param ignoreIfExists
      */
    def createTable(table:CatalogTable, ignoreIfExists:Boolean) : Unit = {
        require(table != null)
        val cmd = CreateTableCommand(table, ignoreIfExists)
        cmd.run(spark)
    }

    /**
      * Returns true if the specified Hive table actually exists
      * @param table
      * @return
      */
    def tableExists(table:TableIdentifier) : Boolean = {
        require(table != null)
        // "SHOW TABLES IN training LIKE 'weather_raw'"
        catalog.tableExists(table)
    }

    /**
      * Returns information about a Hive table
      * @param table
      * @return
      */
    def getTable(table:TableIdentifier) : CatalogTable = {
        require(table != null)
        // "DESCRIBE FORMATTED training.weather_raw"
        catalog.getTableMetadata(table)
    }

    /**
      * Returns the storage location of a Hive table
      * @param table
      * @return
      */
    def getTableLocation(table:TableIdentifier) : Path = {
        require(table != null)
        // "DESCRIBE FORMATTED training.weather_raw"
        new Path(catalog.getTableMetadata(table).location)
    }

    /**
      * Drops a whole table including all partitions and all files
      * @param table
      */
    def dropTable(table:TableIdentifier) : Unit = {
        require(table != null)
        if (tableExists(table)) {
            logger.info(s"Dropping Hive table $table")
            // Delete all partitions
            val catalogTable = catalog.getTableMetadata(table)
            if (catalogTable.partitionSchema != null && catalogTable.partitionSchema.fields.nonEmpty) {
                catalog.listPartitions(table).foreach(p => deleteLocation(new Path(p.location)))
            }

            // Delete table itself
            val location = getTableLocation(table)
            deleteLocation(location)
            val cmd = DropTableCommand(table, false, false, true)
            cmd.run(spark)
        }
    }

    /**
      * Truncates a table by either removing the corresponding file or by dropping all partitions
      * @param table
      */
    @Override
    def truncateTable(table:TableIdentifier) : Unit = {
        require(table != null)
        logger.info(s"Truncating Hive table $table")
        val catalogTable = catalog.getTableMetadata(table)
        val location = new Path(catalogTable.location)
        truncateLocation(location)

        if (catalogTable.partitionSchema != null && catalogTable.partitionSchema.fields.nonEmpty) {
            dropPartitions(table, catalog.listPartitions(table).map(p => PartitionSpec(p.parameters)))
        }
    }

    /**
      * Returns the storage location of a single partition of a Hive table
      * @param table
      * @param partition
      * @return
      */
    @Override
    def partitionExists(table:TableIdentifier, partition:PartitionSpec) : Boolean = {
        require(table != null)
        require(partition != null && partition.nonEmpty)
        catalog.getPartition(table, partition.mapValues(_.toString)) != null
    }

    /**
      * Returns the storage location of a single partition of a Hive table
      * @param table
      * @param partition
      * @return
      */
    @Override
    def getPartitionLocation(table:TableIdentifier, partition:PartitionSpec) : Path = {
        require(table != null)
        require(partition != null && partition.nonEmpty)
        // "DESCRIBE FORMATTED training.weather_raw PARTITION(year=2005, station='x')"
        new Path(catalog.getPartition(table, partition.mapValues(_.toString)).location)
    }

    /**
      * Returns the partition schema of a Hive table. If the table is not partitioned, an empty schema will be returned
      * @param table
      * @return
      */
    @Override
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
    @Override
    def addPartition(table:TableIdentifier, partition:PartitionSpec, location:Path) : Unit = {
        require(table != null)
        require(partition != null && partition.nonEmpty)
        require(location != null && location.toString.nonEmpty)
        logger.info(s"Adding partition $partition to table '$table'")
        val cmd = AlterTableAddPartitionCommand(table, Seq((partition.mapValues(_.toString), Some(location.toString))), false)
        cmd.run(spark)
    }

    /**
      * Adds a new partition or replaces an existing partition to an existing table.
      * @param table
      * @param partition
      * @param location
      */
    @Override
    def addOrReplacePartition(table:TableIdentifier, partition:PartitionSpec, location:Path) : Unit = {
        require(table != null)
        require(partition != null && partition.nonEmpty)
        require(location != null && location.toString.nonEmpty)

        val cmd = if (partitionExists(table, partition)) {
            AlterTableSetLocationCommand(table, Some(partition.mapValues(_.toString)), location.toString)
        }
        else {
            AlterTableAddPartitionCommand(table, Seq((partition.mapValues(_.toString), Some(location.toString))), false)
        }
        cmd.run(spark)
    }

    /**
      * Truncates a single partition by removing all files (but keeping the directory)
      * @param table
      * @param partition
      */
    @Override
    def truncatePartition(table:TableIdentifier, partition:PartitionSpec) : Unit = {
        require(table != null)
        require(partition != null && partition.nonEmpty)
        logger.info(s"Truncating partition $partition of Hive table $table")
        val location = getPartitionLocation(table, partition)
        truncateLocation(location)
    }

    /**
      * Drops a Hive table partition and removes the corresponding directory or file
      * @param table
      * @param partition
      */
    @Override
    def dropPartition(table:TableIdentifier, partition:PartitionSpec) : Unit = {
        require(table != null)
        require(partition != null && partition.nonEmpty)
        dropPartitions(table, Seq(partition))
    }

    @Override
    def dropPartitions(table:TableIdentifier, partitions:Seq[PartitionSpec]) : Unit = {
        require(table != null)
        require(partitions != null)
        logger.info(s"Dropping partitions $partitions of Hive table $table")
        val cmd = new AlterTableDropPartitionCommand(table, partitions.map(_.mapValues(_.toString)), true, true, false)
        cmd.run(spark)
        //partitions.foreach(partition => {
        //    val p = catalog.getPartition(table, partition.mapValues(_.toString))
        //    if (p != null) {
        //        val location = new Path(p.location)
        //        deleteLocation(location)
        //    }
        //}
    }

    private def truncateLocation(location:Path): Unit = {
        val fs = location.getFileSystem(hadoopConf)
        if (fs.isDirectory(location)) {
            logger.info(s"Deleting all files in directory '$location'")
            fs.listStatus(location).foreach(f => fs.delete(f.getPath, true))
        }
        else if (fs.isFile(location)) {
            logger.info(s"Deleting single file '$location'")
            fs.delete(location, false)
        }
    }
    private def deleteLocation(location:Path): Unit = {
        val fs = location.getFileSystem(hadoopConf)
        if (fs.exists(location)) {
            logger.info(s"Deleting file or directory '$location'")
            fs.delete(location, true)
        }
    }
}
