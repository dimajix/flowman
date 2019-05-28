/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.transforms.SchemaEnforcer
import com.dimajix.flowman.types.StructType


case class UpdateMapping(
    instanceProperties:Mapping.Properties,
    input:MappingIdentifier,
    updates:MappingIdentifier,
    keyColumns:Seq[String],
    filter:String = ""
) extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[UpdateMapping])

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param tables
      * @return
      */
    override def execute(executor: Executor, tables: Map[MappingIdentifier, DataFrame]): DataFrame = {
        logger.info(s"Updating table '$input' with records from '$updates' using key columns ${keyColumns.mkString(",")}")
        require(input != null && input.nonEmpty, "Missing input table")
        require(updates != null && updates.nonEmpty, "Missing updates table")
        require(keyColumns.nonEmpty, "Missing key columns")

        val inputDf = tables(input)
        val updatesDf = tables(updates)

        // Apply optional filter to updates (for example for removing DELETEs)
        val filteredUpdates = if (filter != null && filter.nonEmpty) updatesDf.where(filter) else updatesDf

        // Project updates DataFrame to schema of input DataFrame
        val conformer = new SchemaEnforcer(inputDf.schema)
        val projectedUpdates = conformer.transform(filteredUpdates)

        // Perform update operation
        val joinCondition = keyColumns.map(col => inputDf(col) === updatesDf(col)).reduce(_ && _)
        val result = inputDf.join(updatesDf, joinCondition, "left_anti")
            .union(projectedUpdates)

        result
    }

    /**
      * Returns the dependencies of this mapping, which is the input table and the updates table
      *
      * @return
      */
    override def dependencies : Array[MappingIdentifier] = {
        Array(input, updates)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(input:Map[MappingIdentifier,StructType]) : StructType = {
        require(input != null)

        input(this.input)
    }
}



class UpdateMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "updates", required = true) private var updates: String = _
    @JsonProperty(value = "filter", required = false) private var filter: String = ""
    @JsonProperty(value = "keyColumns", required = true) private var keyColumns: Seq[String] = Seq()

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): UpdateMapping = {
        UpdateMapping(
            instanceProperties(context),
            MappingIdentifier(context.evaluate(input)),
            MappingIdentifier.parse(context.evaluate(updates)),
            keyColumns.map(context.evaluate),
            context.evaluate(filter)
        )
    }
}