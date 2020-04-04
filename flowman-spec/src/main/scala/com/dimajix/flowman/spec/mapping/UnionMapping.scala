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

package com.dimajix.flowman.spec.mapping

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
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
    schema:Option[Schema],
    distinct:Boolean,
    filter:Option[String]
) extends BaseMapping {
    /**
      * Creates the list of required dependencies
      *
      * @return
      */
    override def inputs : Seq[MappingOutputIdentifier] = {
        input
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param tables
      * @return
      */
    override def execute(executor:Executor, tables:Map[MappingOutputIdentifier,DataFrame]) : Map[String,DataFrame] = {
        require(executor != null)
        require(tables != null)

        val dfs = inputs.map(tables(_))

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

    override def describe(executor:Executor, input: Map[MappingOutputIdentifier, StructType]): Map[String, StructType] = {
        require(executor != null)
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

        Map("main" -> result)
    }
}



class UnionMappingSpec extends MappingSpec {
    @JsonProperty(value="inputs", required=true) var inputs:Seq[String] = Seq()
    @JsonProperty(value="schema", required=false) var schema:SchemaSpec = _
    @JsonProperty(value="distinct", required=false) var distinct:String = "false"
    @JsonProperty(value = "filter", required=false) private var filter:Option[String] = None

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): UnionMapping = {
        UnionMapping(
            instanceProperties(context),
            inputs.map(i => MappingOutputIdentifier.parse(context.evaluate(i))),
            Option(schema).map(_.instantiate(context)),
            context.evaluate(distinct).toBoolean,
            context.evaluate(filter)
        )
    }
}
