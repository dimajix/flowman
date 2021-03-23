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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.StructType
import org.slf4j.Logger

import com.dimajix.common.Trilean
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.PartitionedRelation
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.util.SchemaUtils


abstract class HiveRelation extends BaseRelation with PartitionedRelation {
    protected val logger:Logger

    def database: Option[String]
    def table: String
    def tableIdentifier: TableIdentifier = new TableIdentifier(table, database)

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param execution
      * @param schema     - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    override def read(execution: Execution, schema: Option[StructType], partitions: Map[String, FieldValue] = Map()): DataFrame = {
        require(execution != null)
        require(schema != null)
        require(partitions != null)

        logger.info(s"Reading Hive relation '$identifier' from table $tableIdentifier using partition values $partitions")

        val reader = execution.spark.read
        val tableDf = reader.table(tableIdentifier.unquotedString)
        val df = filterPartition(tableDf, partitions)

        SchemaUtils.applySchema(df, schema)
    }

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage
      * @param execution
      * @return
      */
    override def exists(execution:Execution) : Trilean = {
        require(execution != null)

        val catalog = execution.catalog
        catalog.tableExists(tableIdentifier)
    }
}
