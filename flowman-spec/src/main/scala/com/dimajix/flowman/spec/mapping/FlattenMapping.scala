/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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
import com.dimajix.flowman.transforms.CaseFormat
import com.dimajix.flowman.transforms.FlattenTransformer
import com.dimajix.flowman.types.StructType


case class FlattenMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
    naming:CaseFormat,
    filter:Option[String] = None
) extends BaseMapping {
    /**
     * Returns the dependencies (i.e. names of tables in the Dataflow model)
     *
     * @return
     */
    override def inputs : Set[MappingOutputIdentifier] = {
        Set(input)
    }

    /**
      * Executes the mapping operation and returns a corresponding DataFrame
      *
      * @param execution
      * @param input
      * @return
      */
    override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]) : Map[String,DataFrame] = {
        require(execution != null)
        require(input != null)

        val df = input(this.input)
        val xfs = FlattenTransformer(naming)

        val result = xfs.transform(df)

        // Apply optional filter
        val filteredResult = filter.map(result.filter).getOrElse(result)

        Map("main" -> filteredResult)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(execution:Execution, input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(execution != null)
        require(input != null)

        val mappingId = this.input
        val schema = input(mappingId)
        val xfs = FlattenTransformer(naming)

        val result = xfs.transform(schema)

        // Apply documentation
        val schemas = Map("main" -> result)
        applyDocumentation(schemas)
    }
}



class FlattenMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "naming", required = true) private var naming:String = "snakeCase"
    @JsonProperty(value = "filter", required=false) private var filter:Option[String] = None

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): FlattenMapping = {
        FlattenMapping(
            instanceProperties(context, properties),
            MappingOutputIdentifier(context.evaluate(input)),
            CaseFormat.ofString(context.evaluate(naming)),
            context.evaluate(filter)
        )
    }
}
