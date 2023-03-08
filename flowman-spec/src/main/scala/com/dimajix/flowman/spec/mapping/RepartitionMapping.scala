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
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.types.StructType


case class RepartitionMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
    columns:Seq[String],
    partitions:Int,
    sort:Boolean = false,
    filter:Option[String] = None
) extends BaseMapping {
    /**
     * Returns the dependencies of this mapping, which is exactly one input table
     *
     * @return
     */
    override def inputs : Set[MappingOutputIdentifier] = {
        Set(input) ++ expressionDependencies(filter)
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param execution
      * @param input
      * @return
      */
    override def execute(execution:Execution, input:Map[MappingOutputIdentifier,DataFrame]) : Map[String,DataFrame] = {
        require(execution != null)
        require(input != null)

        val df = input(this.input)

        // Apply optional filter
        val filtered = applyFilter(df, filter, input)

        val result =
            if (columns.isEmpty) {
                filtered.repartition(partitions)
            }
            else {
                val cols = columns.map(col)
                val repartitioned = if (partitions > 0) filtered.repartition(partitions, cols:_*) else filtered.repartition(cols:_*)
                if (sort)
                    repartitioned.sortWithinPartitions(cols:_*)
                else
                    repartitioned
            }

        Map("main" -> result)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(execution: Execution, input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(execution != null)
        require(input != null)

        val result = input(this.input)

        // Apply documentation
        val schemas = Map("main" -> result)
        applyDocumentation(schemas)
    }
}



class RepartitionMappingSpec extends MappingSpec {
    @JsonProperty(value="input", required=true) private var input: String = _
    @JsonProperty(value="columns", required=false) private[spec] var columns:Seq[String] = Seq()
    @JsonSchemaInject(json="""{"type": [ "integer", "string" ]}""")
    @JsonProperty(value="partitions", required=false) private[spec] var partitions:Option[String] = None
    @JsonSchemaInject(json="""{"type": [ "boolean", "string" ]}""")
    @JsonProperty(value="sort", required=false) private[spec] var sort:String = "false"
    @JsonProperty(value="filter", required=false) private var filter: Option[String] = None

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): RepartitionMapping = {
        RepartitionMapping(
            instanceProperties(context, properties),
            MappingOutputIdentifier(context.evaluate(input)),
            columns.map(context.evaluate),
            partitions.map(context.evaluate).map(_.toInt).getOrElse(0),
            context.evaluate(sort).toBoolean,
            context.evaluate(filter)
        )
    }
}
