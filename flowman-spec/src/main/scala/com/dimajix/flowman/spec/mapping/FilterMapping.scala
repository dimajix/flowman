/*
 * Copyright (C) 2018 The Flowman Authors
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
import com.dimajix.flowman.types.StructType


case class FilterMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
    condition:String
) extends BaseMapping {
    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @return
      */
    override def inputs : Set[MappingOutputIdentifier] = Set(input) ++ expressionDependencies(condition)

    /**
      * Executes this Transform by reading from the specified source and returns a corresponding DataFrame
      *
      * @param execution
      * @param tables
      * @return
      */
    override def execute(execution:Execution, tables:Map[MappingOutputIdentifier,DataFrame]) : Map[String,DataFrame] = {
        require(execution != null)
        require(tables != null)

        val df = tables(input)
        val result = applyFilter(df, Some(condition), tables)

        Map("main" -> result)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(execution:Execution, input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(execution != null)
        require(input != null)

        val result = input(this.input)

        // Apply documentation
        val schemas = Map("main" -> result)
        applyDocumentation(schemas)
    }
}




class FilterMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "condition", required = true) private var condition:String = _

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): FilterMapping = {
        FilterMapping(
            instanceProperties(context, properties),
            MappingOutputIdentifier(context.evaluate(input)),
            context.evaluate(condition)
        )
    }
}
