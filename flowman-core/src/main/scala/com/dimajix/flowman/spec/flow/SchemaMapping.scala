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
import com.dimajix.flowman.spec.schema.Schema
import com.dimajix.flowman.transforms.SchemaEnforcer
import com.dimajix.flowman.types.StructType


object SchemaMapping {
    def apply(input:String, columns:Map[String,String]) : SchemaMapping = {
        val mapping = new SchemaMapping
        mapping._input = input
        mapping._columns = columns
        mapping
    }
}


class SchemaMapping extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[SchemaMapping])

    @JsonProperty(value = "input", required = true) private[spec] var _input:String = _
    @JsonProperty(value = "columns", required = false) private[spec] var _columns:Map[String,String] = Map()
    @JsonProperty(value = "schema", required = false) private var _schema: Schema = _

    def input(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_input))
    def columns(implicit context: Context) : Seq[(String,String)] = _columns.mapValues(context.evaluate).toSeq
    def schema(implicit context: Context): Schema = _schema

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param tables
      * @return
      */
    override def execute(executor:Executor, tables:Map[MappingIdentifier,DataFrame]) : DataFrame = {
        require(executor != null)
        require(tables != null)

        implicit val context = executor.context
        val input = this.input
        val schema = this.schema
        val columns = this.columns

        val xfs = if (schema != null) {
            logger.info(s"Projecting mapping '$input' onto specified schema")
            SchemaEnforcer(schema.sparkSchema)
        }
        else if (columns != null && columns.nonEmpty) {
            logger.info(s"Projecting mapping '$input' onto columns ${columns.map(_._2).mkString(",")}")
            SchemaEnforcer(columns)
        }
        else {
            throw new IllegalArgumentException(s"Require either schema or columns in mapping $name")
        }

        val df = tables(input)
        xfs.transform(df)
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
        StructType(schema.fields)
    }
}
