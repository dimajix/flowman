/*
 * Copyright (C) 2021 The Flowman Authors
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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.dimajix.jackson.ListMapDeserializer

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.schema.SchemaSpec
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.StructType


final case class EmptyMapping(
    instanceProperties:Mapping.Properties,
    columns:Seq[Field] = Seq(),
    schema:Option[Schema]
) extends BaseMapping {
    if (columns.nonEmpty && schema.nonEmpty)
        throw new IllegalArgumentException("Cannot specify both fields and schema in EmptyMapping")
    if (columns.isEmpty && schema.isEmpty)
        throw new IllegalArgumentException("Need either fields or schema in EmptyMapping")

    private lazy val effectiveSchema  = {
        new StructType(schema.map(_.fields).getOrElse(columns))
    }
    private lazy val sparkSchema = effectiveSchema.sparkType

    /**
     * Returns the dependencies (i.e. names of tables in the Dataflow model)
     *
     * @return
     */
    override def inputs: Set[MappingOutputIdentifier] = Set.empty

    /**
     * Executes this Mapping and returns a corresponding map of DataFrames per output
     *
     * @param execution
     * @param input
     * @return
     */
    override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]): Map[String, DataFrame] = {
        val rdd = execution.spark.sparkContext.emptyRDD[Row]
        val df = execution.spark.createDataFrame(rdd, sparkSchema)

        Map("main" -> df)
    }

    /**
     * Returns the schema as produced by this mapping, relative to the given input schema. The map might not contain
     * schema information for all outputs, if the schema cannot be inferred.
     *
     * @param input
     * @return
     */
    override def describe(execution: Execution, input: Map[MappingOutputIdentifier, StructType]): Map[String, StructType] = {
        // Apply documentation
        val schemas = Map("main" -> effectiveSchema)
        applyDocumentation(schemas)
    }
}



class EmptyMappingSpec extends MappingSpec {
    @JsonDeserialize(using = classOf[ListMapDeserializer]) // Old Jackson in old Spark doesn't support ListMap
    @JsonProperty(value = "columns", required=false) private var columns:Map[String,String] = Map()
    @JsonProperty(value = "schema", required = false) protected var schema: Option[SchemaSpec] = None

    /**
     * Creates the instance of the specified Mapping with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): EmptyMapping = {
        EmptyMapping(
            instanceProperties(context, properties),
            context.evaluate(columns).map { case(name,typ) => Field(name, FieldType.of(typ))}.toSeq,
            schema.map(_.instantiate(context))
        )
    }
}
