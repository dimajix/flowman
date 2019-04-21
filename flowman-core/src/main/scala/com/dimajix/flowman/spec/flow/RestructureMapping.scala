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
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier


object RestructureMapping {
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", visible = false)
    @JsonSubTypes(value = Array(
        new JsonSubTypes.Type(name = "column", value = classOf[ColumnEntry]),
        new JsonSubTypes.Type(name = "struct", value = classOf[StructEntry])
    ))
    class Entry {
        @JsonProperty(value = "name", required = false) private var _name:String = _

        def name(implicit context: Context) : String = context.evaluate(_name)
    }

    class ColumnEntry extends Entry {
        @JsonProperty(value = "path", required = false) private var _path:String = _
        @JsonProperty(value = "keep", required = false) private var _keep:Seq[String] = Seq()
        @JsonProperty(value = "drop", required = false) private var _drop:Seq[String] = Seq()

        def path(implicit context: Context) : String = context.evaluate(_path)
        def keep(implicit context: Context) : Seq[String] = _keep.map(context.evaluate)
        def drop(implicit context: Context) : Seq[String] = _drop.map(context.evaluate)
    }

    class StructEntry extends Entry {
        @JsonProperty(value = "columns", required = false) private var _columns:Seq[Entry] = Seq()

        def columns(implicit context: Context) : Seq[Entry] = _columns
    }
}


class RestructureMapping extends BaseMapping {
    import com.dimajix.flowman.spec.flow.RestructureMapping.Entry

    private val logger = LoggerFactory.getLogger(classOf[FilterMapping])

    @JsonProperty(value = "input", required = true) private var _input:String = _
    @JsonProperty(value = "columns", required = false) private var _columns:Seq[Entry] = Seq()

    def input(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_input))
    def columns(implicit context: Context) : Seq[Entry] = _columns

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
      * @param input
      * @return
      */
    override def execute(executor: Executor, input: Map[MappingIdentifier, DataFrame]): DataFrame = ???
}
