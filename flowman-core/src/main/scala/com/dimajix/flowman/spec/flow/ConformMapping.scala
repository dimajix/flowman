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

import java.util.Locale

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.util.SchemaUtils


object ConformMapping {
    def apply(input:String, columns:Map[String,String]) : ConformMapping = {
        val mapping = new ConformMapping
        mapping._input = input
        mapping._columns = columns
        mapping
    }
}


class ConformMapping extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[ConformMapping])

    @JsonProperty(value = "input", required = true) private[spec] var _input:String = _
    @JsonProperty(value = "columns", required = true) private[spec] var _columns:Map[String,String] = Map()

    def input(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_input))
    def columns(implicit context: Context) : Seq[(String,String)] = _columns.mapValues(context.evaluate).toSeq

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param tables
      * @return
      */
    override def execute(executor:Executor, tables:Map[MappingIdentifier,DataFrame]) : DataFrame = {
        implicit val context = executor.context
        val columns = this.columns
        val input = this.input
        logger.info(s"Projecting mapping '$input' onto columns ${columns.map(_._2).mkString(",")}")
        require(input != null && input.nonEmpty, "Require input mapping")

        val df = tables(input)
        val inputCols = df.columns.map(col => (col.toUpperCase(Locale.ROOT), df(col))).toMap
        val cols = columns.map(nv =>
            inputCols.getOrElse(nv._1.toUpperCase(Locale.ROOT), lit(null).as(nv._1))
                .cast(SchemaUtils.mapType(nv._2))
        )
        df.select(cols:_*)
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
