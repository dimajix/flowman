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

import scala.collection.immutable.ListMap

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.spark.sql.DataFrame

import com.dimajix.jackson.ListMapDeserializer

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.transforms.Assembler
import com.dimajix.flowman.transforms.StackTransformer
import com.dimajix.flowman.transforms.schema.Path
import com.dimajix.flowman.types.StructType


case class StackMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
    nameColumn:String,
    valueColumn:String,
    stackColumns:ListMap[String,String],
    dropNulls:Boolean = true,
    keepColumns:Seq[String] = Seq(),
    dropColumns:Seq[String] = Seq(),
    filter:Option[String] = None
) extends BaseMapping {
    /**
     * Returns the dependencies (i.e. names of tables in the Dataflow model)
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
        val result = xfs.transform(df)
        val filteredResult = applyFilter(result, filter, tables)
        val assembledResult = asm.map(_.reassemble(filteredResult)).getOrElse(filteredResult)

        Map("main" -> assembledResult)
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
        val result = xfs.transform(schema)
        val assembledResult = asm.map(_.reassemble(result)).getOrElse(result)

        // Apply documentation
        val schemas = Map("main" -> assembledResult)
        applyDocumentation(schemas)
    }

    private lazy val xfs : StackTransformer =
        StackTransformer(
            nameColumn,
            valueColumn,
            stackColumns,
            dropNulls
        )
    private lazy val asm : Option[Assembler] = {
        if (keepColumns.nonEmpty || dropColumns.nonEmpty) {
            val keep = if (keepColumns.nonEmpty) keepColumns :+ nameColumn :+ valueColumn else Seq()
            Some(
                Assembler.builder()
                    .columns(
                        _.drop(dropColumns.map(c => Path(c)))
                         .keep(keep.map(c => Path(c)))
                    )
                    .build()
            )
        }
        else {
            None
        }
    }
}



class StackMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "nameColumn", required=false) private var nameColumn:String = "name"
    @JsonProperty(value = "valueColumn", required=false) private var valueColumn:String = "value"
    @JsonDeserialize(using = classOf[ListMapDeserializer]) // Old Jackson in old Spark doesn't support ListMap
    @JsonProperty(value = "stackColumns", required = false) private var stackColumns:ListMap[String,String] = ListMap()
    @JsonProperty(value = "keepColumns", required=false) private var keepColumns:Seq[String] = Seq()
    @JsonProperty(value = "dropColumns", required=false) private var dropColumns:Seq[String] = Seq()
    @JsonProperty(value = "dropNulls", required=false) private var dropNulls:String = "false"
    @JsonProperty(value = "filter", required=false) private var filter:Option[String] = None

    /**
     * Creates the instance of the specified Mapping with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context, properties:Option[Mapping.Properties] = None): StackMapping = {
        StackMapping(
            instanceProperties(context, properties),
            MappingOutputIdentifier(context.evaluate(input)),
            context.evaluate(nameColumn),
            context.evaluate(valueColumn),
            ListMap(stackColumns.toSeq.map {case(k,v) => k -> context.evaluate(v) }:_*),
            context.evaluate(dropNulls).toBoolean,
            keepColumns.map(context.evaluate),
            dropColumns.map(context.evaluate),
            context.evaluate(filter)
        )
    }
}
