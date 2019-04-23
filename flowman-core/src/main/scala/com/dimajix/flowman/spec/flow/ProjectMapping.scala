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
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.util.SchemaUtils


object ProjectMapping {
    def apply(input:String, columns:Seq[String]) : ProjectMapping = {
        val mapping = new ProjectMapping
        mapping._input = input
        mapping._columns = columns
        mapping
    }
}


class ProjectMapping extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[ProjectMapping])

    @JsonProperty(value = "input", required = true) private[spec] var _input:String = _
    @JsonProperty(value = "columns", required = true) private[spec] var _columns:Seq[String] = Seq()

    def input(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_input))
    def columns(implicit context: Context) : Seq[String] = _columns.map(context.evaluate)

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
        logger.info(s"Projecting mapping '$input' onto columns ${columns.mkString(",")}")

        val df = tables(input)
        val cols = columns.map(nv => col(nv))
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
        val columns = this.columns
        val mappingId = this.input
        val schema = input(mappingId)
        val inputFields = schema.fields.map(f => (f.name.toLowerCase(Locale.ROOT), f)).toMap
        val outputFields = columns.map { name =>
            inputFields.getOrElse(name.toLowerCase(Locale.ROOT), throw new IllegalArgumentException(s"Cannot find field $name in schema $mappingId"))
        }
        StructType(outputFields)
    }
}
