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
import com.dimajix.flowman.transforms.CaseFormat
import com.dimajix.flowman.transforms.CaseFormatter
import com.dimajix.flowman.transforms.FlattenTransformer
import com.dimajix.flowman.transforms.Transformer
import com.dimajix.flowman.transforms.TypeReplacer
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.StructType


case class ConformMapping(
    instanceProperties: Mapping.Properties,
    input: MappingOutputIdentifier,
    types: Map[String,FieldType] = Map(),
    naming: Option[CaseFormat] = None,
    flatten: Boolean = false,
    filter: Option[String] = None
)
extends BaseMapping {
    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def inputs: Set[MappingOutputIdentifier] = {
        Set(input) ++ expressionDependencies(filter)
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param execution
      * @param input
      * @return
      */
    override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]) : Map[String,DataFrame] = {
        require(execution != null)
        require(input != null)

        val df = input(this.input)
        val transforms = this.transforms

        // Apply all transformations in order
        val result = transforms.foldLeft(df)((df,xfs) => xfs.transform(df))

        // Apply optional filter
        val filteredResult = applyFilter(result, filter, input)

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
        val transforms = this.transforms

        // Apply all transformations in order
        val result = transforms.foldLeft(schema)((df,xfs) => xfs.transform(df))

        // Apply documentation
        val schemas = Map("main" -> result)
        applyDocumentation(schemas)
    }

    private def transforms : Seq[Transformer] = {
        Seq(
            Option(types).filter(_.nonEmpty).map(t => TypeReplacer(t)),
            naming.map(f => CaseFormatter(f)),
            if(flatten) Some(FlattenTransformer(naming.get)) else None
        ).flatten
    }
}


class ConformMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private[spec] var input: String = _
    @JsonProperty(value = "types", required = false) private[spec] var types: Map[String, String] = Map()
    @JsonProperty(value = "naming", required = false) private[spec] var naming: Option[String] = None
    @JsonProperty(value = "flatten", required = false) private[spec] var flatten: String = "false"
    @JsonProperty(value = "filter", required = false) private var filter: Option[String] = None

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): Mapping = {
        ConformMapping(
            instanceProperties(context, properties),
            MappingOutputIdentifier.parse(context.evaluate(input)),
            types.map(kv => kv._1 -> FieldType.of(context.evaluate(kv._2))),
            context.evaluate(naming).filter(_.nonEmpty).map(CaseFormat.ofString),
            context.evaluate(flatten).toBoolean,
            context.evaluate(filter)
        )
    }
}
