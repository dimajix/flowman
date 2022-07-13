/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.hook

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonProperty.Access
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.common.TypeRegistry
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Category
import com.dimajix.flowman.model.Hook
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.spec.Spec
import com.dimajix.flowman.spec.annotation.HookType
import com.dimajix.flowman.spi.ClassAnnotationHandler


object HookSpec extends TypeRegistry[HookSpec] {
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", visible=true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "jdbc", value = classOf[JdbcHookSpec]),
    new JsonSubTypes.Type(name = "simpleReport", value = classOf[SimpleReportHookSpec]),
    new JsonSubTypes.Type(name = "report", value = classOf[ReportHookSpec]),
    new JsonSubTypes.Type(name = "web", value = classOf[WebHookSpec])
))
abstract class HookSpec extends Spec[Hook] {
    @JsonProperty(value="kind", access=Access.WRITE_ONLY, required = true) protected var kind: String = _

    def instantiate(context:Context, properties:Option[Hook.Properties] = None): Hook

    /**
     * Returns a set of common properties
     * @param context
     * @return
     */
    protected def instanceProperties(context:Context, properties:Option[Hook.Properties]) : Hook.Properties = {
        require(context != null)
        val props = Hook.Properties(
            context,
            Metadata(context, kind, Category.HOOK, kind)
        )
        properties.map(p => props.merge(p)).getOrElse(props)
    }
}


class HookSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[HookType]

    override def register(clazz: Class[_]): Unit =
        HookSpec.register(clazz.getAnnotation(classOf[HookType]).kind(), clazz.asInstanceOf[Class[_ <: HookSpec]])
}
