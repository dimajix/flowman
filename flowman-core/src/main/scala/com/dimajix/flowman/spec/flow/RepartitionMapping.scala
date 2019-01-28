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
import org.apache.spark.sql.functions.col

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier

class RepartitionMapping extends BaseMapping {
    @JsonProperty(value = "input", required = true) private var _input:String = _
    @JsonProperty(value = "columns", required = true) private[spec] var _columns:Seq[String] = _
    @JsonProperty(value = "partitions", required = false) private[spec] var _partitions:String = _
    @JsonProperty(value = "sort", required = false) private[spec] var _sort:String = _

    def input(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_input))
    def columns(implicit context: Context) :Seq[String] = if (_columns != null) _columns.map(context.evaluate) else Seq[String]()
    def partitions(implicit context: Context) : Int= if (_partitions == null || _partitions.isEmpty) 0 else context.evaluate(_partitions).toInt
    def sort(implicit context: Context) : Boolean = if (_sort == null || _sort.isEmpty) false else context.evaluate(_sort).toBoolean

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor:Executor, input:Map[MappingIdentifier,DataFrame]) : DataFrame = {
        implicit val context = executor.context
        val df = input(this.input)
        val parts = partitions
        val cols = columns.map(col)
        val repartitioned = if (parts > 0) df.repartition(parts, cols:_*) else df.repartition(cols:_*)
        if (sort)
            repartitioned.sortWithinPartitions(cols:_*)
        else
            repartitioned
    }

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context: Context) : Array[MappingIdentifier] = {
        Array(input)
    }
}
