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
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.types.ArrayRecord
import com.dimajix.flowman.types.MapRecord
import com.dimajix.flowman.types.Record
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.types.ValueRecord
import com.dimajix.spark.sql.DataFrameBuilder
import com.dimajix.spark.sql.DataFrameUtils


case class MockMapping(
    instanceProperties:Mapping.Properties,
    mapping:MappingIdentifier,
    records:Seq[Record] = Seq()
) extends BaseMapping {
    private lazy val mocked = context.getMapping(mapping, false)

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
        val schemas = describe(execution, Map())
        if (records.nonEmpty) {
            if (schemas.size != 1)
                throw new UnsupportedOperationException("MockMapping only supports a single output with specified records")
            val (name,schema) = schemas.head

            val values = records.map(_.toArray(schema))
            val df = DataFrameBuilder.ofStringValues(execution.spark, values, schema.sparkType)
            Map(name -> df)
        }
        else {
            schemas.map { case (name, schema) =>
                val df = DataFrameBuilder.ofSchema(execution.spark, schema.sparkType)
                (name, df)
            }
        }
    }


    /**
     * Creates an output identifier for the primary output
     *
     * @return
     */
    override def output: MappingOutputIdentifier = {
        MappingOutputIdentifier(identifier, mocked.output.output)
    }

    /**
     * Lists all outputs of this mapping. Every mapping should have one "main" output
     *
     * @return
     */
    override def outputs: Seq[String] = mocked.outputs

    /**
     * Returns the schema as produced by this mapping, relative to the given input schema. The map might not contain
     * schema information for all outputs, if the schema cannot be inferred.
     *
     * @param input
     * @return
     */
    override def describe(execution: Execution, input: Map[MappingOutputIdentifier, StructType]): Map[String, StructType] = {
        mocked.outputs.map(out => out -> describe(execution, Map(), out)).toMap
    }

    /**
     * Returns the schema as produced by this mapping, relative to the given input schema. If the schema cannot
     * be inferred, None will be returned
     *
     * @param input
     * @return
     */
    override def describe(execution: Execution, input: Map[MappingOutputIdentifier, StructType], output: String): StructType = {
        require(execution != null)
        require(input != null)
        require(output != null && output.nonEmpty)

        execution.describe(mocked, output)
    }
}


class MockMappingSpec extends MappingSpec {
    @JsonProperty(value = "mapping", required=false) private var mapping:Option[String] = None
    @JsonProperty(value = "records", required=false) private var records:Seq[Record] = Seq()

    /**
     * Creates the instance of the specified Mapping with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context): MockMapping = {
        MockMapping(
            instanceProperties(context),
            MappingIdentifier(context.evaluate(mapping).getOrElse(name)),
            records.map(_.map(context.evaluate))
        )
    }
}
