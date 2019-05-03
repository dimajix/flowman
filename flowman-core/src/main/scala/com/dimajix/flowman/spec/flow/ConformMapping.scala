/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.flow

import java.util.Locale

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.transforms.CaseFormatter
import com.dimajix.flowman.transforms.Transformer
import com.dimajix.flowman.transforms.TypeReplacer
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.StructType


object ConformMapping {
    private val typeAliases = Map(
        "text" -> "string",
        "long" -> "bigint",
        "short" -> "tinyint"
    )
    def apply(input:String, types:Map[String,String]) : ConformMapping = {
        val result = new ConformMapping
        result._input = input
        result._types = types
        result
    }
    def apply(input:String, caseFormat:String) : ConformMapping = {
        val result = new ConformMapping
        result._input = input
        result._naming = caseFormat
        result
    }
}


class ConformMapping extends BaseMapping {
    import ConformMapping.typeAliases
    private val logger = LoggerFactory.getLogger(classOf[ProjectMapping])

    @JsonProperty(value = "input", required = true) private[spec] var _input:String = _
    @JsonProperty(value = "types", required = false) private[spec] var _types:Map[String,String] = Map()
    @JsonProperty(value = "naming", required = false) private[spec] var _naming:String = _

    def input(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_input))
    def types(implicit context: Context) : Map[String,FieldType] = _types.map(kv =>
        typeAliases.getOrElse(kv._1.toLowerCase(Locale.ROOT), kv._1) -> FieldType.of(context.evaluate(kv._2))
    )
    def naming(implicit context: Context) : String = context.evaluate(_naming)

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor: Executor, input: Map[MappingIdentifier, DataFrame]): DataFrame = {
        require(executor != null)
        require(input != null)

        implicit val icontext = executor.context
        val mappingId = this.input
        val df = input(mappingId)
        val transforms = this.transforms

        // Apply all transformations in order
        transforms.foldLeft(df)((df,xfs) => xfs.transform(df))
    }

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context: Context): Array[MappingIdentifier] = Array(input)

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param context
      * @param input
      * @return
      */
    override def describe(context:Context, input:Map[MappingIdentifier,StructType]) : StructType = {
        require(context != null)
        require(input != null)

        implicit val icontext = context
        val mappingId = this.input
        val schema = input(mappingId)
        val transforms = this.transforms

        // Apply all transformations in order
        transforms.foldLeft(schema)((df,xfs) => xfs.transform(df))
    }

    private def transforms(implicit context: Context) : Seq[Transformer] = {
        Seq(
            Option(types).filter(_.nonEmpty).map(t => TypeReplacer(t)),
            Option(naming).filter(_.nonEmpty).map(f => CaseFormatter(f))
        ).flatten
    }
}
