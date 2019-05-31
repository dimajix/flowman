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
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.types.StructType


case class DistinctMapping(
   instanceProperties:Mapping.Properties,
   input:MappingOutputIdentifier
) extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[DistinctMapping])

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @return
      */
    override def dependencies : Seq[MappingOutputIdentifier] = {
        Seq(input)
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param tables
      * @return
      */
    override def execute(executor: Executor, tables: Map[MappingOutputIdentifier, DataFrame]) : Map[String,DataFrame] = {
        require(executor != null)
        require(input != null)

        logger.info(s"Filtering distinct rows from mapping '$input'")

        val df = tables(input)
        val result = df.distinct()

        Map("default" -> result)
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


class DistinctMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _

    override def instantiate(context: Context): DistinctMapping = {
        DistinctMapping(
            instanceProperties(context),
            MappingOutputIdentifier(context.evaluate(input))
        )
    }
}
