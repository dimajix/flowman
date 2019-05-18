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

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.AbstractInstance
import com.dimajix.flowman.spec.Instance
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spi.TypeRegistry
import com.dimajix.flowman.state.TargetInstance


object Target {
    case class Properties(
        context:Context,
        name:String="",
        kind:String="",
        labels:Map[String,String]=Map(),
        enabled: Boolean=true,
        input: MappingIdentifier=MappingIdentifier.empty
     ) extends Instance.Properties
}


abstract class Target extends AbstractInstance {
    /**
      * Returns the category of this resource
      * @return
      */
    final override def category: String = "target"

    /**
      * Returns true if the output should be executed per default
      * @return
      */
    def enabled : Boolean

    /**
      * Returns an instance representing this target with the context
      * @return
      */
    def instance : TargetInstance

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @return
      */
    def dependencies : Array[MappingIdentifier]

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




object TargetSpec extends TypeRegistry[TargetSpec] {
    type NameResolver = NamedSpec.NameResolver[Target, TargetSpec]
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", visible = true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "blackhole", value = classOf[BlackholeTargetSpec]),
    new JsonSubTypes.Type(name = "count", value = classOf[CountTargetSpec]),
    new JsonSubTypes.Type(name = "console", value = classOf[ConsoleTargetSpec]),
    new JsonSubTypes.Type(name = "local", value = classOf[LocalTargetSpec]),
    new JsonSubTypes.Type(name = "relation", value = classOf[RelationTargetSpec]),
    new JsonSubTypes.Type(name = "stream", value = classOf[StreamTargetSpec]))
)
abstract class TargetSpec extends NamedSpec[Target] {
    @JsonProperty(value = "enabled", required=false) private var enabled:String = "true"
    @JsonProperty(value = "input", required=true) private var input:String = _

    override def instantiate(context: Context): Target

    /**
      * Returns a set of common properties
      * @param context
      * @return
      */
    override protected def instanceProperties(context:Context) : Target.Properties = {
        Target.Properties(
            context,
            name,
            kind,
            labels.mapValues(context.evaluate),
            context.evaluate(enabled).toBoolean,
            MappingIdentifier.parse(context.evaluate(input))
        )
    }
}
