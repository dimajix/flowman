/*
 * Copyright 2021 Kaya Kupferschmidt
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
import org.apache.spark.sql.Row

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


case class NullMapping(
    instanceProperties:Mapping.Properties,
    fields:Seq[Field] = Seq(),
    schema:Option[Schema]
) extends BaseMapping {
    if (fields.nonEmpty && schema.nonEmpty)
        throw new IllegalArgumentException("Cannot specify both fields and schema in NullMapping")
    if (fields.isEmpty && schema.isEmpty)
        throw new IllegalArgumentException("Need either fields or schema in NullMapping")

    private lazy val effectiveSchema  = {
        new StructType(schema.map(_.fields).getOrElse(fields))
    }
    private lazy val sparkSchema = effectiveSchema.sparkType

    /**
     * Returns the dependencies (i.e. names of tables in the Dataflow model)
     *
     * @return
     */
    override def inputs: Seq[MappingOutputIdentifier] = Seq()

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
        Map("main" -> effectiveSchema)
    }
}



class NullMappingSpec extends MappingSpec {
    @JsonProperty(value = "fields", required=false) private var fields:Map[String,String] = Map()
    @JsonProperty(value = "schema", required = false) protected var schema: Option[SchemaSpec] = None

    /**
     * Creates the instance of the specified Mapping with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context): NullMapping = {
        NullMapping(
            instanceProperties(context),
            context.evaluate(fields).map { case(name,typ) => Field(name, FieldType.of(typ))}.toSeq,
            schema.map(_.instantiate(context))
        )
    }
}
