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

package com.dimajix.flowman.spec.documentation

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.common.TypeRegistry
import com.dimajix.flowman.documentation.Generator
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.Spec
import com.dimajix.flowman.spec.annotation.GeneratorType
import com.dimajix.flowman.spi.ClassAnnotationHandler


object GeneratorSpec extends TypeRegistry[GeneratorSpec] {
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "file", value = classOf[FileGeneratorSpec])
))
abstract class GeneratorSpec extends Spec[Generator] {
    def instantiate(context:Context): Generator
}


class GeneratorSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[GeneratorType]

    override def register(clazz: Class[_]): Unit =
        GeneratorSpec.register(clazz.getAnnotation(classOf[GeneratorType]).kind(), clazz.asInstanceOf[Class[_ <: GeneratorSpec]])
}
