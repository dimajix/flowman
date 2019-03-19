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
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.util.StdConverter
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Resource
import com.dimajix.flowman.spi.TypeRegistry
import com.dimajix.flowman.state.TargetInstance


object Target extends TypeRegistry[Target] {
    class NameResolver extends StdConverter[Map[String,Target],Map[String,Target]] {
        override def convert(value: Map[String,Target]): Map[String,Target] = {
            value.foreach(kv => kv._2._name = kv._1)
            value
        }
    }
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", visible = true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "blackhole", value = classOf[BlackholeTarget]),
    new JsonSubTypes.Type(name = "count", value = classOf[CountTarget]),
    new JsonSubTypes.Type(name = "console", value = classOf[ConsoleTarget]),
    new JsonSubTypes.Type(name = "local", value = classOf[LocalTarget]),
    new JsonSubTypes.Type(name = "relation", value = classOf[RelationTarget]),
    new JsonSubTypes.Type(name = "stream", value = classOf[StreamTarget]))
)
abstract class Target extends Resource {
    @JsonIgnore private var _name:String = ""

    @JsonProperty(value="kind", required = true) private var _kind: String = _
    @JsonProperty(value="labels", required=false) private var _labels:Map[String,String] = Map()

    /**
      * Returns the name of the output
      * @return
      */
    final override def name : String = _name

    /**
      * Returns the category of this resource
      * @return
      */
    final override def category: String = "target"

    /**
      * Returns the specific kind of this resource
      * @return
      */
    final override def kind: String = _kind

    /**
      * Returns a map of user defined labels
      * @param context
      * @return
      */
    final override def labels(implicit context: Context) : Map[String,String] = _labels.mapValues(context.evaluate)

    /**
      * Returns true if the output should be executed per default
      * @param context
      * @return
      */
    def enabled(implicit context:Context) : Boolean

    /**
      * Returns an instance representing this target with the context
      * @param context
      * @return
      */
    def instance(implicit context: Context) : TargetInstance

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
    def build(executor:Executor, input:Map[MappingIdentifier,DataFrame]) : Unit

    /**
      * Cleans up a specific target
      *
      * @param executor
      */
    def clean(executor:Executor) : Unit
}
