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
import org.apache.spark.sql.functions

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier


case class SelectMapping(
    instanceProperties:Mapping.Properties,
    input:MappingIdentifier,
    columns:Seq[(String,String)]
)
extends BaseMapping {
    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor:Executor, input:Map[MappingIdentifier,DataFrame]) : DataFrame = {
        val df = input(this.input)
        val cols = columns.map { case (name,value) => functions.expr(value).as(name) }
        df.select(cols:_*)
    }

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @return
      */
    override def dependencies : Array[MappingIdentifier] = {
        require(context != null)

        Array(input)
    }
}



class SelectMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "columns", required = false) private var columns:Map[String,String] = Map()

    override def instantiate(context: Context): SelectMapping = {
        SelectMapping(
            instanceProperties(context),
            MappingIdentifier(context.evaluate(this.input)),
            columns.mapValues(context.evaluate).toSeq
        )
    }
}
