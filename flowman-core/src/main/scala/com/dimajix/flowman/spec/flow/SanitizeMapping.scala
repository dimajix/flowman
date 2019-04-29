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
import com.dimajix.flowman.transforms.TypeReplacer
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.StructType


object SanitizeMapping {
    private val typeAliases = Map(
        "text" -> "string",
        "long" -> "bigint",
        "short" -> "tinyint"
    )
    def apply(input:String, types:Map[String,String]) : SanitizeMapping = {
        val result = new SanitizeMapping
        result._input = input
        result._types = types
        result
    }
}


class SanitizeMapping extends BaseMapping {
    import SanitizeMapping.typeAliases
    private val logger = LoggerFactory.getLogger(classOf[ProjectMapping])

    @JsonProperty(value = "input", required = true) private[spec] var _input:String = _
    @JsonProperty(value = "types") private[spec] var _types:Map[String,String] = Map()

    def input(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_input))
    def types(implicit context: Context) : Map[String,FieldType] = _types.map(kv =>
        typeAliases.getOrElse(kv._1.toLowerCase(Locale.ROOT), kv._1) -> FieldType.of(context.evaluate(kv._2))
    )

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
        val types = this.types
        val mappingId = this.input
        val df = input(mappingId)

        val xfs = new TypeReplacer(types)
        xfs.transform(df)
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
        val types = this.types
        val mappingId = this.input
        val schema = input(mappingId)

        val xfs = new TypeReplacer(types)
        xfs.transform(schema)
    }
}
