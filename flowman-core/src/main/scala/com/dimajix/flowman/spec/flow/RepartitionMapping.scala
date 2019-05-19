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
import org.apache.spark.sql.functions.col

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.types.StructType


case class RepartitionMapping(
    instanceProperties:Mapping.Properties,
    input:MappingIdentifier,
    columns:Seq[String],
    partitions:Int,
    sort:Boolean
) extends BaseMapping {
    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor:Executor, input:Map[MappingIdentifier,DataFrame]) : DataFrame = {
        val df = input(this.input)
        val cols = columns.map(col)
        val repartitioned = if (partitions > 0) df.repartition(partitions, cols:_*) else df.repartition(cols:_*)
        if (sort)
            repartitioned.sortWithinPartitions(cols:_*)
        else
            repartitioned
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



class RepartitionMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "columns", required = true) private[spec] var columns:Seq[String] = _
    @JsonProperty(value = "partitions", required = false) private[spec] var partitions:String = _
    @JsonProperty(value = "sort", required = false) private[spec] var sort:String = _

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): RepartitionMapping = {
        RepartitionMapping(
            instanceProperties(context),
            MappingIdentifier(context.evaluate(input)),
            columns.map(context.evaluate),
            context.evaluate(partitions).toInt,
            context.evaluate(sort).toBoolean
        )
    }
}
