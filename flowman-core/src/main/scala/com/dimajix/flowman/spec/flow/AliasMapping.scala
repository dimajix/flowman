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

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.types.StructType


case class AliasMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier
) extends BaseMapping {
    /**
      * Executes this mapping by returning a DataFrame which corresponds to the specified input
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor:Executor, input:Map[MappingOutputIdentifier,DataFrame]) : Map[String,DataFrame] = {
        val result = input(this.input)
        Map("default" -> result)
    }

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @return
      */
    override def dependencies : Seq[MappingOutputIdentifier] = {
        Seq(input)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(input != null)
        val result = input(this.input)
        Map("default" -> result)
    }
}


class AliasMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): AliasMapping = {
        AliasMapping(
            instanceProperties(context),
            MappingOutputIdentifier(context.evaluate(input))
        )
    }
}
