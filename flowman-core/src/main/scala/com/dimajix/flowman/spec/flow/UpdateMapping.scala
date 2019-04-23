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

package com.dimajix.flowman.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.transforms.Conformer
import com.dimajix.flowman.types.StructType


object UpdateMapping {
    def apply(input:String, updates:String, keyColumns:Seq[String], filter:String="") : UpdateMapping = {
        val mapping = new UpdateMapping
        mapping._input = input
        mapping._updates = updates
        mapping._keyColumns = keyColumns
        mapping._filter = filter
        mapping
    }
}


class UpdateMapping extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[UpdateMapping])

    @JsonProperty(value = "input", required = true) private var _input: String = _
    @JsonProperty(value = "updates", required = true) private var _updates: String = _
    @JsonProperty(value = "filter", required = false) private var _filter: String = _
    @JsonProperty(value = "keyColumns", required = true) private var _keyColumns: Seq[String] = Seq()

    def input(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_input))
    def updates(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_updates))
    def filter(implicit context: Context) : String = context.evaluate(_filter)
    def keyColumns(implicit context: Context) : Seq[String] = _keyColumns.map(context.evaluate)

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param tables
      * @return
      */
    override def execute(executor: Executor, tables: Map[MappingIdentifier, DataFrame]): DataFrame = {
        implicit val context = executor.context
        val input = this.input
        val updates = this.updates
        val filter = this.filter
        val keyColumns = this.keyColumns

        logger.info(s"Updating table '$input' with records from '$updates' using key columns ${keyColumns.mkString(",")}")
        require(input != null && input.nonEmpty, "Missing input table")
        require(updates != null && updates.nonEmpty, "Missing updates table")
        require(keyColumns.nonEmpty, "Missing key columns")

        val inputDf = tables(input)
        val updatesDf = tables(updates)

        // Apply optional filter to updates (for example for removing DELETEs)
        val filteredUpdates = if (filter != null && filter.nonEmpty) updatesDf.where(filter) else updatesDf

        // Project updates DataFrame to schema of input DataFrame
        val projectedUpdates = Conformer.conformSchema(filteredUpdates, inputDf.schema)

        // Perform update operation
        inputDf.join(updatesDf, keyColumns, "left_anti")
            .union(projectedUpdates)
    }

    /**
      * Returns the dependencies of this mapping, which is the input table and the updates table
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context: Context) : Array[MappingIdentifier] = {
        Array(input, updates)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param context
      * @param input
      * @return
      */
    override def describe(context:Context, input:Map[MappingIdentifier,StructType]) : StructType = {
        require(context != null)
        require(input != null)

        implicit val icontext = context
        input(this.input)
    }
}
