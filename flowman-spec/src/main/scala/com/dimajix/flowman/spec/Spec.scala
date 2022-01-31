/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.util.StdConverter

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Category
import com.dimajix.flowman.model.Instance
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.model.Prototype


trait Spec[T] extends Prototype[T] {
    def instantiate(context:Context) : T
}

trait ToSpec[T] {
    def spec : T
}

object NamedSpec {
    class NameResolver[S <: NamedSpec[_]] extends StdConverter[Map[String, S], Map[String, S]] {
        override def convert(value: Map[String, S]): Map[String, S] = {
            value.foreach(kv => kv._2.name = kv._1)
            value
        }
    }
}


final class MetadataSpec {
    @JsonProperty(value="labels", required=false) protected var labels:Map[String,String] = Map()

    def instantiate(context:Context, name:String, category:Category, kind:String) : Metadata = {
        Metadata(
            namespace = context.namespace.map(_.name),
            project = context.project.map(_.name),
            name = name,
            version = context.project.flatMap(_.version),
            category = category.lower,
            kind = kind,
            labels = context.evaluate(labels)
        )
    }
}


abstract class NamedSpec[T] extends Spec[T] {
    @JsonProperty(value="name", required = false) protected[spec] var name:String = ""
    @JsonProperty(value="metadata", required=false) protected var metadata:Option[MetadataSpec] = None

    override def instantiate(context:Context) : T

    /**
      * Returns a set of common properties
      * @param context
      * @return
      */
    protected def instanceProperties(context:Context) : Instance.Properties[_]
}
