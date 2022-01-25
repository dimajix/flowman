/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.measure

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.annotation.JsonTypeResolver

import com.dimajix.common.TypeRegistry
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Category
import com.dimajix.flowman.model.Measure
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spec.annotation.MeasureType
import com.dimajix.flowman.spec.template.CustomTypeResolverBuilder
import com.dimajix.flowman.spi.ClassAnnotationHandler


object MeasureSpec extends TypeRegistry[MeasureSpec] {
    final class NameResolver extends NamedSpec.NameResolver[MeasureSpec]
}


@JsonTypeResolver(classOf[CustomTypeResolverBuilder])
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", visible = true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "sql", value = classOf[SqlMeasureSpec])
))
abstract class MeasureSpec  extends NamedSpec[Measure] {
    @JsonProperty(value="description", required = false) private var description: Option[String] = None

    override def instantiate(context: Context): Measure

    override protected def instanceProperties(context:Context) : Measure.Properties = {
        require(context != null)
        val name = context.evaluate(this.name)
        Measure.Properties(
            context,
            metadata.map(_.instantiate(context, name, Category.MEASURE, kind)).getOrElse(Metadata(context, name, Category.MEASURE, kind)),
            context.evaluate(description)
        )
    }
}


class MeasureSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[MeasureType]

    override def register(clazz: Class[_]): Unit =
        MeasureSpec.register(clazz.getAnnotation(classOf[MeasureType]).kind(), clazz.asInstanceOf[Class[_ <: MeasureSpec]])
}
