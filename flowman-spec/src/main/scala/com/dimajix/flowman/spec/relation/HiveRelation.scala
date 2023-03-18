/*
 * Copyright (C) 2018 The Flowman Authors
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

import com.dimajix.common.Trilean
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.PartitionedRelation
import com.dimajix.flowman.types.FieldValue


abstract class HiveRelation extends BaseRelation with PartitionedRelation {
    def table: TableIdentifier

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param execution
      * @param schema     - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    override def read(execution: Execution, partitions: Map[String, FieldValue] = Map()): DataFrame = {
        require(execution != null)
        require(partitions != null)

        logger.info(s"Reading Hive relation '$identifier' from table $table using partition values $partitions")

        val reader = execution.spark.read
        val tableDf = reader.table(table.unquotedString)
        val filteredDf = filterPartition(tableDf, partitions)

        applyInputSchema(execution, filteredDf)
    }

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage
      * @param execution
      * @return
      */
    override def exists(execution:Execution) : Trilean = {
        require(execution != null)

        val catalog = execution.catalog
        catalog.tableExists(table)
    }
}
