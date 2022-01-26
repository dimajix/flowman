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

package com.dimajix.flowman.spec.storage

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.common.TypeRegistry
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.Spec
import com.dimajix.flowman.spec.annotation.StoreType
import com.dimajix.flowman.spi.ClassAnnotationHandler
import com.dimajix.flowman.storage.Store


object StoreSpec extends TypeRegistry[StoreSpec] {
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "file", value = classOf[FileStoreSpec]),
    new JsonSubTypes.Type(name = "null", value = classOf[NullStoreSpec])
))
abstract class StoreSpec extends Spec[Store] {
    override def instantiate(context:Context): Store
}



class StoreSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[StoreType]

    override def register(clazz: Class[_]): Unit =
        StoreSpec.register(clazz.getAnnotation(classOf[StoreType]).kind(), clazz.asInstanceOf[Class[_ <: StoreSpec]])
}
