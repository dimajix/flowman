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
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions.expr
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier


class ExtendMapping extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[ExtendMapping])

    @JsonProperty(value = "input", required = true) private[spec] var _input:String = _
    @JsonProperty(value = "columns", required = true) private[spec] var _columns:Map[String,String] = _

    def input(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_input))
    def columns(implicit context: Context) : Map[String,String] = _columns.mapValues(context.evaluate)

    /**
      * Executes this Transform by reading from the specified source and returns a corresponding DataFrame
      *
      * @param executor
      * @param tables
      * @return
      */
    override def execute(executor:Executor, tables:Map[MappingIdentifier,DataFrame]) : DataFrame = {
        implicit val context = executor.context
        val allColumns = this.columns
        val columnNames = allColumns.keys.toSet
        val input = this.input

        logger.info(s"Extending mapping '$input' with columns ${columnNames.mkString("[",",","]")}")

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
                if (columnNames.contains(field) && !ordered.contains(field))
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
        val table = tables(input)
        orderedFields._1.foldLeft(table)((df,field) => df.withColumn(field, expr(allColumns(field))))
    }

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context: Context) : Array[MappingIdentifier] = {
        Array(input)
    }
}
