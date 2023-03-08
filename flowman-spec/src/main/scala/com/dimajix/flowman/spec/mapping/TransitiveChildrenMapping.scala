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

package com.dimajix.flowman.spec.mapping

import java.util.Locale

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.types.StructType


case class TransitiveChildrenMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
    parentColumns:Seq[String],
    childColumns:Seq[String],
    includeSelf:Boolean = true,
    filter:Option[String] = None
) extends BaseMapping {
    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def inputs: Set[MappingOutputIdentifier] = Set(input) ++ expressionDependencies(filter)

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param execution
      * @param input
      * @return
      */
    override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]): Map[String, DataFrame] = {
        require(execution != null)
        require(input != null)

        def iterate(df:DataFrame) : DataFrame = {
            val leftAlias = "left"
            val rightAlias = "right"
            val left = df.alias(leftAlias)
            val right = df.alias(rightAlias)

            val cols = parentColumns.zip(childColumns)
            val condition = cols.map(c => col(leftAlias + "." + c._2) === col(rightAlias + "." + c._1)).reduce(_ && _)

            left.join(right, condition, "inner")
                .select(parentColumns.map(c => col(leftAlias + "." + c)) ++ childColumns.map(c => col(rightAlias + "." + c)):_*)
                .union(
                    left.select(parentColumns.map(c => col(leftAlias + "." + c)) ++ childColumns.map(c => col(leftAlias + "." + c)):_*)
                )
                .distinct()
                // Create checkpoint to prevent too large execution plans
                .localCheckpoint()
        }

        val allColumns = parentColumns.map(col) ++ childColumns.map(col)

        // Project onto relevant columns
        val df = input(this.input)
            .select(allColumns:_*)
        // Filter our all NULL values in child and parent IDs
        val cleanedDf = df.filter(allColumns.map(_.isNotNull).reduce(_ && _))

        // Iteratively add more parent-child relations until number of records doesn't change any more
        var nextDf = cleanedDf.localCheckpoint()
        var nextCount = nextDf.count()
        var count = 0L
        while (nextCount != count) {
            nextDf = iterate(nextDf)
            count = nextCount
            nextCount = nextDf.count()
        }

        // Include self-references if required
        val result =
            if (includeSelf) {
                val cols = parentColumns.zip(childColumns)
                // First put in parent_id as child_id (i.e. parent-parent-relations)
                val s1 = df.filter(parentColumns.map(c => col(c).isNotNull).reduce(_ && _))
                    .select(parentColumns.map(col) ++ cols.map(pc => col(pc._1).as(pc._2)):_*)

                // Now put in child_id as parent_id (i.e. child-child-relations)
                val s2 = df.filter(childColumns.map(c => col(c).isNotNull).reduce(_ && _))
                    .select(cols.map(pc => col(pc._2).as(pc._1)) ++ childColumns.map(col):_*)

                // Create UNION DISTINCT from all sets
                nextDf.union(s1).union(s2).distinct()
            }
            else {
                nextDf
            }

        // Apply optional filter
        val filteredResult = applyFilter(result, filter, input)

        Map("main" -> filteredResult)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      *
      * @param input
      * @return
      */
    override def describe(execution:Execution, input: Map[MappingOutputIdentifier, StructType]): Map[String, StructType] = {
        require(execution != null)
        require(input != null)

        val schema = input(this.input)

        // Project onto parent and child columns
        val fieldsByName = schema.fields.map(field => (field.name.toLowerCase(Locale.ROOT), field)).toMap
        val columns = parentColumns.map(n => fieldsByName(n.toLowerCase(Locale.ROOT))) ++
            childColumns.map(n => fieldsByName(n.toLowerCase(Locale.ROOT)))
        val result = StructType(columns)

        // Apply documentation
        val schemas = Map("main" -> result)
        applyDocumentation(schemas)
    }
}


class TransitiveChildrenMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "includeSelf", required = true) private var includeSelf: String = "true"
    @JsonProperty(value = "parentColumns", required = true) private var parentColumns: Seq[String] = Seq.empty
    @JsonProperty(value = "childColumns", required = true) private var childColumns: Seq[String] = Seq.empty
    @JsonProperty(value = "filter", required=false) private var filter:Option[String] = None

    /**
      * Creates an instance of this specification and performs the interpolation of all variables
      *
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): TransitiveChildrenMapping = {
        TransitiveChildrenMapping(
            instanceProperties(context, properties),
            MappingOutputIdentifier(context.evaluate(input)),
            parentColumns.map(context.evaluate),
            childColumns.map(context.evaluate),
            context.evaluate(includeSelf).toBoolean,
            context.evaluate(filter)
        )
    }
}
