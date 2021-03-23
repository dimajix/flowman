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

package com.dimajix.flowman.spec.mapping

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier


case class JoinMapping(
    instanceProperties:Mapping.Properties,
    input:Seq[MappingOutputIdentifier],
    columns:Seq[String] = Seq(),
    condition:String = "",
    mode:String = "left",
    filter:Option[String] = None
) extends BaseMapping {
    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def inputs : Seq[MappingOutputIdentifier] = {
        input
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param execution
      * @param tables
      * @return
      */
    override def execute(execution: Execution, tables: Map[MappingOutputIdentifier, DataFrame]): Map[String,DataFrame] = {
        require(execution != null)
        require(tables != null)

        val result = if (condition.nonEmpty) {
            require(inputs.size == 2, "Joining using an condition only supports exactly two inputs")

            val left = inputs(0)
            val right = inputs(1)
            val leftDf = tables(left).as(left.name)
            val rightDf = tables(right).as(right.name)
            leftDf.join(rightDf, expr(condition), mode)
        }
        else {
            inputs.map(tables.apply).reduceLeft((l, r) => l.join(r, columns, mode))
        }

        // Apply optional filter
        val filteredResult = filter.map(result.filter).getOrElse(result)

        Map("main" -> filteredResult)
    }
}


class JoinMappingSpec extends MappingSpec {
    @JsonProperty(value = "inputs", required = true) private var inputs:Seq[String] = Seq()
    @JsonProperty(value = "columns", required = false) private var columns:Seq[String] = Seq()
    @JsonProperty(value = "condition", required = false) private var expression:String = ""
    @JsonProperty(value = "mode", required = true) private var mode:String = "left"
    @JsonProperty(value = "filter", required=false) private var filter:Option[String] = None

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): JoinMapping = {
        JoinMapping(
            instanceProperties(context),
            inputs.map(id => MappingOutputIdentifier(context.evaluate(id))),
            columns.map(context.evaluate),
            context.evaluate(expression),
            context.evaluate(mode),
            context.evaluate(filter)
        )
    }
}
