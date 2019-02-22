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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable

import com.dimajix.flowman.spec.schema.PartitionSchema

abstract class Catalog {
    /**
      * Returns true if the specified Hive table actually exists
      * @param table
      * @return
      */
    def tableExists(table:TableIdentifier) : Boolean


    /**
      * Returns information about a Hive table
      * @param table
      * @return
      */
    def getTable(table:TableIdentifier) : CatalogTable

    /**
      * Creates a new table from a detailed definition
      * @param table
      * @param ignoreIfExists
      */
    def createTable(table:CatalogTable, ignoreIfExists:Boolean) : Unit

    /**
      * Returns the storage location of a Hive table
      * @param table
      * @return
      */
    def getTableLocation(table:TableIdentifier) : Path

    /**
      * Returns the storage location of a single partition of a Hive table
      * @param table
      * @param partition
      * @return
      */
    def partitionExists(table:TableIdentifier, partition:PartitionSpec) : Boolean

    /**
      * Returns the storage location of a single partition of a Hive table
      * @param table
      * @param partition
      * @return
      */
    def getPartitionLocation(table:TableIdentifier, partition:PartitionSpec) : Path

    /**
      * Returns the partition schema of a Hive table. If the table is not partitioned, an empty schema will be returned
      * @param table
      * @return
      */
    def getPartitionSchema(table:TableIdentifier) : PartitionSchema

    /**
      * Drops a whole table including all partitions and all files
      * @param table
      */
    def dropTable(table:TableIdentifier) : Unit

    /**
      * Truncates a table by either removing the corresponding file or by dropping all partitions
      * @param table
      */
    def truncateTable(table:TableIdentifier) : Unit

    /**
      * Adds a new partition to an existing table. Will throw an exception if the partition already exists
      * @param table
      * @param partition
      * @param location
      */
    def addPartition(table:TableIdentifier, partition:PartitionSpec, location:Path) : Unit

    /**
      * Adds a new partition or replaces an existing partition to an existing table.
      * @param table
      * @param partition
      * @param location
      */
    def addOrReplacePartition(table:TableIdentifier, partition:PartitionSpec, location:Path) : Unit

    /**
      * Truncates a single partition by removing all files (but keeping the directory)
      * @param table
      * @param partition
      */
    def truncatePartition(table:TableIdentifier, partition:PartitionSpec) : Unit

    /**
      * Drops a Hive table partition and removes the corresponding directory or file
      * @param table
      * @param partition
      */
    def dropPartition(table:TableIdentifier, partition:PartitionSpec) : Unit

    def dropPartitions(table:TableIdentifier, partitions:Seq[PartitionSpec]) : Unit
}
