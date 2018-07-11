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
import org.apache.spark.sql.functions.expr
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier


object JoinMapping {
    def apply(inputs:Seq[String], columns:Seq[String], mode:String) : JoinMapping = {
        val mapping = new JoinMapping
        mapping._inputs = inputs
        mapping._columns = columns
        mapping._mode = mode
        mapping
    }

    def apply(inputs:Seq[String], expr:String, mode:String) : JoinMapping = {
        val mapping = new JoinMapping
        mapping._inputs = inputs
        mapping._expression = expr
        mapping._mode = mode
        mapping
    }
}


class JoinMapping extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[JoinMapping])

    @JsonProperty(value = "inputs", required = true) private var _inputs:Seq[String] = Seq()
    @JsonProperty(value = "columns", required = false) private var _columns:Seq[String] = Seq()
    @JsonProperty(value = "expression", required = false) private var _expression:String = _
    @JsonProperty(value = "mode", required = true) private var _mode:String = "left"

    def inputs(implicit context:Context) : Seq[MappingIdentifier] = _inputs.map(s => MappingIdentifier.parse(context.evaluate(s)))
    def columns(implicit context: Context) : Seq[String] = _columns.map(context.evaluate)
    def expression(implicit context: Context) : String = context.evaluate(_expression)
    def mode(implicit context: Context) : String = context.evaluate(_mode)

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context: Context): Array[MappingIdentifier] = {
        inputs.toArray
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor: Executor, input: Map[MappingIdentifier, DataFrame]): DataFrame = {
        implicit val context = executor.context

        val expression = this.expression
        val inputs = this.inputs
        if (expression != null && expression.nonEmpty) {
            logger.info(s"Joining mappings ${input.mkString(",")} on $expression with type $mode")
            if (inputs.size != 2) {
                logger.error("Joining using an expression only supports exactly two inputs")
                throw new IllegalArgumentException("Joining using an expression only supports exactly two inputs")
            }
            val left = inputs(0)
            val right = inputs(1)
            val leftDf = input(left).as(left.name)
            val rightDf = input(right).as(right.name)
            leftDf.join(rightDf, expr(expression), mode)
        }
        else {
            logger.info(s"Joining mappings ${input.mkString(",")} on columns ${columns.mkString("[",",","]")} with type $mode")
            inputs.map(input.apply).reduceLeft((l, r) => l.join(r, columns, mode))
        }
    }
}
