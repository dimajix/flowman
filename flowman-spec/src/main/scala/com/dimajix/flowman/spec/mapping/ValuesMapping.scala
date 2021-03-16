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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.schema.SchemaSpec
import com.dimajix.flowman.types.ArrayRecord
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.MapRecord
import com.dimajix.flowman.types.Record
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.types.ValueRecord
import com.dimajix.spark.sql.DataFrameUtils


case class ValuesMapping(
    instanceProperties:Mapping.Properties,
    columns:Seq[Field] = Seq(),
    schema:Option[Schema] = None,
    records:Seq[Record] = Seq()
) extends BaseMapping {
    if (schema.isEmpty && columns.isEmpty)
        throw new IllegalArgumentException(s"Require either schema or columns in mapping $name")

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
        val recordsSchema = StructType(schema.map(_.fields).getOrElse(columns))
        val sparkSchema = recordsSchema.sparkType

        val values = records.map(_.toArray(recordsSchema))
        val df = DataFrameUtils.ofStringValues(execution.spark, values, sparkSchema)
        Map("main" -> df)
    }


    /**
     * Creates an output identifier for the primary output
     *
     * @return
     */
    override def output: MappingOutputIdentifier = {
        MappingOutputIdentifier(identifier, "main")
    }

    /**
     * Lists all outputs of this mapping. Every mapping should have one "main" output
     *
     * @return
     */
    override def outputs: Seq[String] = Seq("main")

    /**
     * Returns the schema as produced by this mapping, relative to the given input schema. The map might not contain
     * schema information for all outputs, if the schema cannot be inferred.
     *
     * @param input
     * @return
     */
    override def describe(execution: Execution, input: Map[MappingOutputIdentifier, StructType]): Map[String, StructType] = {
        Map("main" -> StructType(schema.map(_.fields).getOrElse(columns)))
    }
}


class ValuesMappingSpec extends MappingSpec {
    @JsonProperty(value = "schema", required=false) private var schema:Option[SchemaSpec] = None
    @JsonProperty(value = "columns", required = false) private var columns:Map[String,String] = Map()
    @JsonProperty(value = "records", required=false) private var records:Seq[Record] = Seq()

    /**
     * Creates the instance of the specified Mapping with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context): ValuesMapping = {
        ValuesMapping(
            instanceProperties(context),
            context.evaluate(columns).toSeq.map(kv => Field(kv._1, FieldType.of(kv._2))),
            schema.map(_.instantiate(context)),
            records.map(_.map(context.evaluate))
        )
    }
}
