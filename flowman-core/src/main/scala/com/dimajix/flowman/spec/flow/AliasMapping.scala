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
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.types.StructType


case class AliasMapping(
    instanceProperties:Mapping.Properties,
    input:MappingIdentifier
) extends BaseMapping {
    /**
      * Executes this mapping by returning a DataFrame which corresponds to the specified input
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor:Executor, input:Map[MappingIdentifier,DataFrame]) : DataFrame = {
        input(this.input)
    }

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @return
      */
    override def dependencies : Array[MappingIdentifier] = {
        Array(input)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(input:Map[MappingIdentifier,StructType]) : StructType = {
        require(input != null)
        input(this.input)
    }
}


class AliasMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _

    override def instantiate(context: Context): AliasMapping = {
        val props = instanceProperties(context)
        val input = MappingIdentifier(context.evaluate(this.input))
        AliasMapping(props, input)
    }
}
