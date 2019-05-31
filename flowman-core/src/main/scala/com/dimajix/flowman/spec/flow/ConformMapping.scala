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
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.transforms.CaseFormatter
import com.dimajix.flowman.transforms.FlattenTransformer
import com.dimajix.flowman.transforms.Transformer
import com.dimajix.flowman.transforms.TypeReplacer
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.StructType


case class ConformMapping(
    instanceProperties:Mapping.Properties,
    input : MappingOutputIdentifier,
    types : Map[String,FieldType] = Map(),
    naming : String = null,
    flatten : Boolean = false
)
extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[ProjectMapping])

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor: Executor, input: Map[MappingOutputIdentifier, DataFrame]) : Map[String,DataFrame] = {
        require(executor != null)
        require(input != null)

        val df = input(this.input)
        val transforms = this.transforms

        // Apply all transformations in order
        val result = transforms.foldLeft(df)((df,xfs) => xfs.transform(df))

        Map("default" -> result)
    }

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def dependencies: Seq[MappingOutputIdentifier] = {
        Seq(input)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(input != null)

        val schema = input(this.input)
        val transforms = this.transforms

        // Apply all transformations in order
        val result = transforms.foldLeft(schema)((df,xfs) => xfs.transform(df))

        Map("default" -> result)
    }

    private def transforms : Seq[Transformer] = {
        Seq(
            Option(types).filter(_.nonEmpty).map(t => TypeReplacer(t)),
            Option(naming).filter(_.nonEmpty).map(f => CaseFormatter(f)),
            Option(flatten).filter(_ == true).map(_ => FlattenTransformer(Option(naming).filter(_.nonEmpty).getOrElse("snakeCase")))
        ).flatten
    }
}


object ConformMappingSpec {
    private val typeAliases = Map(
        "text" -> "string",
        "long" -> "bigint",
        "short" -> "tinyint"
    )
}
class ConformMappingSpec extends MappingSpec {
    import ConformMappingSpec.typeAliases

    @JsonProperty(value = "input", required = true) private[spec] var input: String = _
    @JsonProperty(value = "types", required = false) private[spec] var types: Map[String, String] = Map()
    @JsonProperty(value = "naming", required = false) private[spec] var naming: String = _
    @JsonProperty(value = "flatten", required = false) private[spec] var flatten: String = "false"

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): Mapping = {
        val types = this.types.map(kv =>
            typeAliases.getOrElse(kv._1.toLowerCase(Locale.ROOT), kv._1) -> FieldType.of(context.evaluate(kv._2))
        )
        ConformMapping(
            instanceProperties(context),
            MappingOutputIdentifier.parse(context.evaluate(input)),
            types,
            context.evaluate(naming),
            context.evaluate(flatten).toBoolean
        )
    }
}
