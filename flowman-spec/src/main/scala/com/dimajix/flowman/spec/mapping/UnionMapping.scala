/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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
import com.dimajix.flowman.transforms.SchemaEnforcer
import com.dimajix.flowman.transforms.UnionTransformer
import com.dimajix.flowman.types.StructType


case class UnionMapping(
    instanceProperties:Mapping.Properties,
    input:Seq[MappingOutputIdentifier],
    schema:Option[Schema] = None,
    distinct:Boolean = false,
    filter:Option[String] = None
) extends BaseMapping {
    /**
      * Creates the list of required dependencies
      *
      * @return
      */
    override def inputs : Set[MappingOutputIdentifier] = {
        input.toSet
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

        val dfs = input.map(tables(_))

        // Now create a union of all tables
        val union =
            if (schema.nonEmpty) {
                // Project all tables onto specified schema
                val schemaEnforcer = SchemaEnforcer(schema.get.sparkSchema)
                val projectedTables = dfs.map(schemaEnforcer.transform)
                projectedTables.reduce((l,r) => l.union(r))
            }
            else {
                // Dynamically create common schema
                val xfs = UnionTransformer()
                xfs.transformDataFrames(dfs)
            }

        // Optionally perform distinct operation
        val result =
            if (distinct)
                union.distinct()
            else
                union

        // Apply optional filter
        val filteredResult = filter.map(result.filter).getOrElse(result)

        Map("main" -> filteredResult)
    }

    override def describe(execution:Execution, input: Map[MappingOutputIdentifier, StructType]): Map[String, StructType] = {
        require(execution != null)
        require(input != null)

        val result =
            if (schema.nonEmpty) {
                StructType(schema.get.fields)
            }
            else {
                val xfs = UnionTransformer()
                val schemas = input.values.toSeq
                xfs.transformSchemas(schemas)
            }

        // Apply documentation
        val schemas = Map("main" -> result)
        applyDocumentation(schemas)
    }
}



class UnionMappingSpec extends MappingSpec {
    @JsonProperty(value="inputs", required=true) var inputs:Seq[String] = Seq()
    @JsonProperty(value="schema", required=false) var schema:SchemaSpec = _
    @JsonProperty(value="distinct", required=false) var distinct:String = "false"
    @JsonProperty(value="filter", required=false) private var filter:Option[String] = None

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): UnionMapping = {
        UnionMapping(
            instanceProperties(context, properties),
            inputs.map(i => MappingOutputIdentifier.parse(context.evaluate(i))),
            Option(schema).map(_.instantiate(context)),
            context.evaluate(distinct).toBoolean,
            context.evaluate(filter)
        )
    }
}
