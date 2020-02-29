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
import com.dimajix.flowman.transforms.Assembler
import com.dimajix.flowman.types.StructType


case class DropMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
    columns:Seq[String],
    filter:Option[String] = None
) extends BaseMapping {
    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def inputs: Seq[MappingOutputIdentifier] = {
        Seq(input)
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param deps
      * @return
      */
    override def execute(executor: Executor, deps: Map[MappingOutputIdentifier, DataFrame]): Map[String,DataFrame] = {
        require(executor != null)
        require(deps != null)

        val df = deps(input)
        val asm = assembler
        val result = asm.reassemble(df)

        // Apply optional filter
        val filteredResult = filter.map(result.filter).getOrElse(result)

        Map("main" -> filteredResult)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param deps
      * @return
      */
    override def describe(executor:Executor, deps:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(executor != null)
        require(deps != null)

        val schema = deps(this.input)
        val asm = assembler
        val result = asm.reassemble(schema)

        Map("main" -> result)
    }

    private def assembler : Assembler = {
        val builder = Assembler.builder()
                .columns(_.drop(columns))
        builder.build()
    }
}


class DropMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "columns", required = true) private var columns: Seq[String] = Seq()
    @JsonProperty(value = "filter", required=false) private var filter:Option[String] = None

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): DropMapping = {
        DropMapping(
            instanceProperties(context),
            MappingOutputIdentifier(context.evaluate(input)),
            columns.map(context.evaluate),
            context.evaluate(filter)
        )
    }
}
