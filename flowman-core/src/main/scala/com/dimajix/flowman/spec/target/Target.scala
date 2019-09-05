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

package com.dimajix.flowman.spec.target

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.util.StdConverter

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.history.TargetInstance
import com.dimajix.flowman.spec.AbstractInstance
import com.dimajix.flowman.spec.Instance
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.ResourceIdentifier
import com.dimajix.flowman.spec.TargetIdentifier
import com.dimajix.flowman.spi.TypeRegistry


object Target {
    object Properties {
        def apply(context:Context, name:String="", kind:String="") : Properties = {
            Properties(
                context,
                context.namespace,
                context.project,
                name,
                kind,
                Map(),
                true,
                Seq(),
                Seq()
            )
        }
    }
    case class Properties(
        context:Context,
        namespace:Namespace,
        project:Project,
        name:String,
        kind:String,
        labels:Map[String,String],
        enabled: Boolean,
        before: Seq[TargetIdentifier],
        after: Seq[TargetIdentifier]
    ) extends Instance.Properties
}


abstract class Target extends AbstractInstance {
    /**
      * Returns the category of this resource
      * @return
      */
    final override def category: String = "target"

    /**
      * Returns an identifier for this target
      * @return
      */
    def identifier : TargetIdentifier

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
      * Returns an explicit user defined list of targets to be executed after this target. I.e. this
      * target needs to be executed before all other targets in this list.
      * @return
      */
    def before : Seq[TargetIdentifier]

    /**
      * Returns an explicit user defined list of targets to be executed before this target I.e. this
      * * target needs to be executed after all other targets in this list.
      *
      * @return
      */
    def after : Seq[TargetIdentifier]

    /**
      * Returns a list of physical resources produced by this target
      * @return
      */
    def produces : Seq[ResourceIdentifier]

    /**
      * Returns a list of physical resources required by this target
      * @return
      */
    def requires : Seq[ResourceIdentifier]

    /**
      * Creates the resource associated with this target. This may be a Hive table or a JDBC table. This method
      * will not provide the data itself, it will only create the container
      * @param executor
      */
    def create(executor:Executor) : Unit
    def migrate(executor:Executor) : Unit

    /**
      * Abstract method which will perform the output operation. All required tables need to be
      * registered as temporary tables in the Spark session before calling the execute method.
      *
      * @param executor
      */
    def build(executor:Executor) : Unit

    /**
      * Deletes data of a specific target
      *
      * @param executor
      */
    def truncate(executor:Executor) : Unit

    /**
      * Completely destroys the resource associated with this target. This will delete both the phyiscal data and
      * the table definition
      * @param executor
      */
    def destroy(executor:Executor) : Unit
}




object TargetSpec extends TypeRegistry[TargetSpec] {
    class NameResolver extends StdConverter[Map[String, TargetSpec], Map[String, TargetSpec]] {
        override def convert(value: Map[String, TargetSpec]): Map[String, TargetSpec] = {
            value.foreach(kv => kv._2.name = kv._1)
            value
        }
    }
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", visible = true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "blackhole", value = classOf[BlackholeTargetSpec]),
    new JsonSubTypes.Type(name = "count", value = classOf[CountTargetSpec]),
    new JsonSubTypes.Type(name = "console", value = classOf[ConsoleTargetSpec]),
    new JsonSubTypes.Type(name = "file", value = classOf[FileTargetSpec]),
    new JsonSubTypes.Type(name = "local", value = classOf[LocalTargetSpec]),
    new JsonSubTypes.Type(name = "relation", value = classOf[RelationTargetSpec]),
    new JsonSubTypes.Type(name = "stream", value = classOf[StreamTargetSpec]))
)
abstract class TargetSpec extends NamedSpec[Target] {
    @JsonProperty(value = "enabled", required=false) private var enabled:String = "true"
    @JsonProperty(value = "before", required=false) private var before:Seq[String] = Seq()
    @JsonProperty(value = "after", required=false) private var after:Seq[String] = Seq()

    override def instantiate(context: Context): Target

    /**
      * Returns a set of common properties
      * @param context
      * @return
      */
    override protected def instanceProperties(context:Context) : Target.Properties = {
        require(context != null)
        Target.Properties(
            context,
            context.namespace,
            context.project,
            name,
            kind,
            context.evaluate(labels),
            context.evaluate(enabled).toBoolean,
            before.map(context.evaluate).map(TargetIdentifier.parse),
            after.map(context.evaluate).map(TargetIdentifier.parse)
        )
    }
}
