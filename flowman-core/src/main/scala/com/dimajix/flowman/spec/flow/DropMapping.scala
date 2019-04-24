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
import com.dimajix.flowman.transforms.Assembler
import com.dimajix.flowman.types.StructType


object DropMapping {
    def apply(input:String, columns:Seq[String]) : DropMapping = {
        val result = new DropMapping
        result._input = input
        result._columns = columns
        result
    }
}


class DropMapping extends BaseMapping {
    private val logger = LoggerFactory.getLogger(classOf[DropMapping])

    @JsonProperty(value = "input", required = true) private var _input:String = _
    @JsonProperty(value = "columns", required = false) private var _columns:Seq[String] = Seq()

    def input(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_input))
    def columns(implicit context: Context) : Seq[String] = _columns.map(context.evaluate)

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context: Context): Array[MappingIdentifier] = {
        Array(input)
    }

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param deps
      * @return
      */
    override def execute(executor: Executor, deps: Map[MappingIdentifier, DataFrame]): DataFrame = {
        require(executor != null)
        require(deps != null)

        implicit val context = executor.context
        val input = this.input
        val columns = this.columns
        logger.info(s"Dropping columns ${columns.mkString(",")} from input mapping '$input'")

        val df = deps(input)
        val asm = assembler
        asm.reassemble(df)
    }

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param context
      * @param deps
      * @return
      */
    override def describe(context:Context, deps:Map[MappingIdentifier,StructType]) : StructType = {
        require(context != null)
        require(deps != null)

        implicit val icontext = context
        val schema = deps(this.input)
        val asm = assembler
        asm.reassemble(schema)
    }

    private def assembler(implicit context:Context) : Assembler = {
        val builder = Assembler.builder()
                .columns(_.drop(columns))
        builder.build()
    }
}
