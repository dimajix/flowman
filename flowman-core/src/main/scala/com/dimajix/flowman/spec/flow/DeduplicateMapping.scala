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
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier


class DeduplicateMapping extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[DeduplicateMapping])

    @JsonProperty(value = "input", required = true) private[spec] var _input:String = _
    @JsonProperty(value = "columns", required = true) private[spec] var _columns:Array[String] = Array()

    def input(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_input))
    def columns(implicit context: Context) : Seq[String] = _columns.map(context.evaluate)

    /**
      * Creates an instance of the deduplication table.
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor:Executor, input:Map[MappingIdentifier,DataFrame]): DataFrame = {
        implicit val context = executor.context
        logger.info("Deduplicating table {} on columns {}", Array(input, columns.mkString(",")):_*)

        val df = input(this.input)
        val cols = if (columns.nonEmpty) columns else df.columns.toSeq
        df.dropDuplicates(cols)
    }

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context:Context) : Array[MappingIdentifier] = {
        Array(input)
    }
}
