/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions.expr

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier


case class ExtendMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
    columns:Map[String,String],
    filter:Option[String] = None
) extends BaseMapping {
    /**
     * Returns the dependencies of this mapping, which is exactly one input table
     *
     * @return
     */
    override def inputs : Set[MappingOutputIdentifier] = {
        Set(input)
    }

    /**
      * Executes this Transform by reading from the specified source and returns a corresponding DataFrame
      *
      * @param execution
      * @param deps
      * @return
      */
    override def execute(execution:Execution, deps:Map[MappingOutputIdentifier,DataFrame]) : Map[String,DataFrame] = {
        require(execution != null)
        require(deps != null)

        val allColumns = this.columns
        val columnNames = allColumns.keys.toSet

        // First we need to create an ordering of all fields, such that dependencies are resolved correctly
        val parser = CatalystSqlParser
        def getRequiredColumns(column:String) = {
            val expression = allColumns(column)
            val result = parser.parseExpression(expression)
            result.references.map(_.name).toSet
        }
        def addField(column:String, orderedFields:Seq[String], usedFields:Set[String]) : (Seq[String], Set[String]) = {
            if (usedFields.contains(column))
                throw new RuntimeException("Cycling dependency between fields.")
            val deps = getRequiredColumns(column)
            val start = (orderedFields, usedFields + column)
            val result = deps.foldLeft(start) { case ((ordered, used), field) =>
                if (field != column && columnNames.contains(field) && !ordered.contains(field))
                    addField(field, ordered, used)
                else
                    (ordered, used)
            }

            (result._1 :+ column, result._2)
        }

        val start = (Seq[String](), Set[String]())
        val orderedFields = columnNames.foldLeft(start) { case ((ordered, used), field) =>
            if (!ordered.contains(field))
                addField(field, ordered, used)
            else
                (ordered, used)
        }

        // Now that we have a field order, we can transform the DataFrame
        val table = deps(input)
        val result = orderedFields._1.foldLeft(table)((df,field) => df.withColumn(field, expr(allColumns(field))))

        // Apply optional filter
        val filteredResult = filter.map(result.filter).getOrElse(result)

        Map("main" -> filteredResult)
    }
}



class ExtendMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "columns", required = true) private var columns: Map[String,String] = Map()
    @JsonProperty(value = "filter", required=false) private var filter:Option[String] = None

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): ExtendMapping = {
        ExtendMapping(
            instanceProperties(context, properties),
            MappingOutputIdentifier(context.evaluate(input)),
            context.evaluate(columns),
            context.evaluate(filter)
        )
    }
}
