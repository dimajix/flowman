/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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
import com.dimajix.flowman.spec.schema.SchemaSpec
import com.dimajix.flowman.transforms.SchemaEnforcer
import com.dimajix.flowman.types.StructType


case class SchemaMapping(
    instanceProperties:Mapping.Properties,
    input:MappingIdentifier,
    columns:Seq[(String,String)] = Seq(),
    schema:Schema = null
)
extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[SchemaMapping])

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
      * @return
      */
    override def dependencies : Array[MappingIdentifier] = {
        Array(input)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(input:Map[MappingIdentifier,StructType]) : StructType = {
        require(input != null)

        StructType(schema.fields)
    }
}



class SchemaMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "columns", required = false) private var columns:Map[String,String] = Map()
    @JsonProperty(value = "schema", required = false) private var schema: SchemaSpec = _

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): SchemaMapping = {
        SchemaMapping(
            instanceProperties(context),
            MappingIdentifier(context.evaluate(this.input)),
            columns.mapValues(context.evaluate).toSeq,
            if (schema != null) schema.instantiate(context) else null
        )
    }
}
