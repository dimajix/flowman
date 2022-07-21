/*
 * Copyright 2022 Kaya Kupferschmidt
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

import com.dimajix.common.MapIgnoreCase
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.StructType


case class CastMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
    columns:Map[String,FieldType],
    filter:Option[String] = None
)
extends BaseMapping {
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
      * @param tables
      * @return
      */
    override def execute(execution:Execution, tables:Map[MappingOutputIdentifier,DataFrame]) : Map[String,DataFrame] = {
        require(execution != null)
        require(tables != null)

        val df = tables(input)
        val colMap = MapIgnoreCase(columns)
        val cols = df.columns.map { name =>
            colMap.get(name).map(dt => df(name).cast(dt.sparkType).as(name)).getOrElse(df(name))
        }
        val result = df.select(cols:_*)

        // Apply optional filter
        val filteredResult = applyFilter(result, filter, tables)

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

        val schema = input(this.input)
        val colMap = MapIgnoreCase(columns)
        val cols = schema.fields.map { field =>
            colMap.get(field.name).map(dt => field.copy(ftype = dt)).getOrElse(field)
        }
        val result = StructType(cols)

        // Apply documentation
        val schemas = Map("main" -> result)
        applyDocumentation(schemas)
    }
}


class CastMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "columns", required = true) private var columns: Map[String,String] = Map.empty
    @JsonProperty(value = "filter", required=false) private var filter: Option[String] = None

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): CastMapping = {
        CastMapping(
            instanceProperties(context, properties),
            MappingOutputIdentifier(context.evaluate(input)),
            columns.map { case(name,value) => name -> FieldType.of(context.evaluate(value)) },
            context.evaluate(filter)
        )
    }
}
