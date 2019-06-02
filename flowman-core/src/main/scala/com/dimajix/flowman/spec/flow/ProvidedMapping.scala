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

package com.dimajix.flowman.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingOutputIdentifier


case class ProvidedMapping(
    instanceProperties:Mapping.Properties,
    table:String
)
extends BaseMapping {
    /**
      * Instantiates the specified table, which must be available in the Spark session
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor:Executor, input:Map[MappingOutputIdentifier,DataFrame]): Map[String,DataFrame] = {
        require(executor != null)
        require(input != null)

        val result = executor.spark.table(table)

        Map("main" -> result)
    }

    /**
      * Returns the dependencies of this mapping, which are empty for an ReadRelationMapping
      *
      * @return
      */
    override def dependencies : Seq[MappingOutputIdentifier] = {
        Seq()
    }
}


class ProvidedMappingSpec extends MappingSpec {
    @JsonProperty(value = "table", required = true) private var table: String = _

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): ProvidedMapping = {
        ProvidedMapping(
            instanceProperties(context),
            context.evaluate(table)
        )
    }
}
