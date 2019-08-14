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
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.types.StructType


case class RepartitionMapping(
    instanceProperties:Mapping.Properties,
    input:MappingOutputIdentifier,
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
    override def execute(executor:Executor, input:Map[MappingOutputIdentifier,DataFrame]) : Map[String,DataFrame] = {
        require(executor != null)
        require(input != null)

        val df = input(this.input)
        val result =
            if (columns.isEmpty) {
                df.repartition(partitions)
            }
            else {
                val cols = columns.map(col)
                val repartitioned = if (partitions > 0) df.repartition(partitions, cols:_*) else df.repartition(cols:_*)
                if (sort)
                    repartitioned.sortWithinPartitions(cols:_*)
                else
                    repartitioned
            }

        Map("main" -> result)
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

        Map("main" -> result)
    }
}



class RepartitionMappingSpec extends MappingSpec {
    @JsonProperty(value = "input", required = true) private var input: String = _
    @JsonProperty(value = "columns", required = false) private[spec] var columns:Seq[String] = Seq()
    @JsonProperty(value = "partitions", required = true) private[spec] var partitions:Option[String] = None
    @JsonProperty(value = "sort", required = false) private[spec] var sort:String = "false"

    /**
      * Creates the instance of the specified Mapping with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): RepartitionMapping = {
        RepartitionMapping(
            instanceProperties(context),
            MappingOutputIdentifier(context.evaluate(input)),
            columns.map(context.evaluate),
            partitions.map(context.evaluate).map(_.toInt).getOrElse(0),
            context.evaluate(sort).toBoolean
        )
    }
}
