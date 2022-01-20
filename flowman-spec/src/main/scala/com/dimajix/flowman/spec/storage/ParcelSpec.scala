/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.storage

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.common.TypeRegistry
import com.dimajix.flowman.hadoop.File
import com.dimajix.flowman.spec.annotation.ParcelType
import com.dimajix.flowman.spi.ClassAnnotationHandler
import com.dimajix.flowman.storage.Parcel


object ParcelSpec extends TypeRegistry[ParcelSpec] {
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", visible = true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "file", value = classOf[LocalParcelSpec]),
    new JsonSubTypes.Type(name = "local", value = classOf[LocalParcelSpec])
))
abstract class ParcelSpec {
    @JsonProperty(value="kind", required = true) protected var kind: String = _
    @JsonProperty(value="name", required = false) protected[spec] var name:String = ""

    def instantiate(root:File) : Parcel
}


class ParcelSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[ParcelType]

    override def register(clazz: Class[_]): Unit =
        ParcelSpec.register(clazz.getAnnotation(classOf[ParcelType]).kind(), clazz.asInstanceOf[Class[_ <: ParcelSpec]])
}
