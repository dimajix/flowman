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
import com.dimajix.flowman.types.StructType


case class RebalanceMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
    partitions:Int
) extends BaseMapping {
    /**
     * Returns the dependencies of this mapping, which is exactly one input table
     *
     * @return
     */
    override def inputs : Seq[MappingOutputIdentifier] = {
        Seq(input)
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor:Executor, input:Map[MappingOutputIdentifier,DataFrame]) : Map[String,DataFrame] = {
        require(executor != null)
        require(input != null)

        val df = input(this.input)
        val result = df.repartition(partitions)

        Map("main" -> result)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    override def describe(executor:Executor, input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(executor != null)
        require(input != null)

        val result = input(this.input)
        Map("main" -> result)
    }
}



class RebalanceMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "partitions", required = true) private[spec] var partitions: String = _

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): RebalanceMapping = {
        RebalanceMapping(
            instanceProperties(context),
            MappingOutputIdentifier.parse(context.evaluate(input)),
            context.evaluate(partitions).toInt
        )
    }
}
