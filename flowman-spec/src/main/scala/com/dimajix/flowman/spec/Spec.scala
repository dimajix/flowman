/*
 * Copyright 2019 Kaya Kupferschmidt
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

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.util.StdConverter

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Instance
import com.dimajix.flowman.model.Prototype


trait Spec[T] extends Prototype[T] {
    def instantiate(context:Context) : T
}


object NamedSpec {
    class NameResolver[S <: NamedSpec[_]] extends StdConverter[Map[String, S], Map[String, S]] {
        override def convert(value: Map[String, S]): Map[String, S] = {
            value.foreach(kv => kv._2.name = kv._1)
            value
        }
    }
}


abstract class NamedSpec[T] extends Spec[T] {
    @JsonIgnore protected var name:String = ""

    @JsonProperty(value="kind", required = true) protected var kind: String = _
    @JsonProperty(value="labels", required=false) protected var labels:Map[String,String] = Map()

    override def instantiate(context:Context) : T

    /**
      * Returns a set of common properties
      * @param context
      * @return
      */
    protected def instanceProperties(context:Context) : Instance.Properties[_]
}
