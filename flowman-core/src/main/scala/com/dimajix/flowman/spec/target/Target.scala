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

package com.dimajix.flowman.spec.target

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.util.StdConverter
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spi.ExtensionRegistry


object Target extends ExtensionRegistry[Target] {
    class NameResolver extends StdConverter[Map[String,Target],Map[String,Target]] {
        override def convert(value: Map[String,Target]): Map[String,Target] = {
            value.foreach(kv => kv._2._name = kv._1)
            value
        }
    }
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "blackhole", value = classOf[BlackholeTarget]),
    new JsonSubTypes.Type(name = "count", value = classOf[CountTarget]),
    new JsonSubTypes.Type(name = "dump", value = classOf[DumpTarget]),
    new JsonSubTypes.Type(name = "local", value = classOf[LocalTarget]),
    new JsonSubTypes.Type(name = "relation", value = classOf[RelationTarget]),
    new JsonSubTypes.Type(name = "stream", value = classOf[StreamTarget]))
)
abstract class Target {
    @JsonIgnore private var _name:String = ""

    /**
      * Returns the name of the output
      * @return
      */
    def name : String = _name

    /**
      * Returns true if the output should be executed per default
      * @param context
      * @return
      */
    def enabled(implicit context:Context) : Boolean

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @param context
      * @return
      */
    def dependencies(implicit context: Context) : Array[MappingIdentifier]

    /**
      * Abstract method which will perform the output operation. All required tables need to be
      * registered as temporary tables in the Spark session before calling the execute method.
      *
      * @param executor
      */
    def execute(executor:Executor, input:Map[MappingIdentifier,DataFrame]) : Unit
}
