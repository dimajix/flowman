/*
 * Copyright 2019 Kaya Kupferschmidt
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


abstract class TemplateMapping extends BaseMapping {
    @JsonProperty(value = "mapping", required = true) private var _mapping:String = _
    @JsonProperty(value = "environment", required = true) private var _environment:Seq[String] = Seq()

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def dependencies: Array[MappingIdentifier] = {
        ???
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor: Executor, input: Map[MappingIdentifier, DataFrame]) : DataFrame = {
        require(executor != null)
        require(input != null)
        ???
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(input:Map[MappingIdentifier,StructType]) : StructType = {
        require(context != null)
        require(input != null)
        ???
    }
}

