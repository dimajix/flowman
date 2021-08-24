/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.assertion

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.common.TypeRegistry
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spec.annotation.AssertionType
import com.dimajix.flowman.spi.ClassAnnotationHandler


object AssertionSpec extends TypeRegistry[AssertionSpec] {
    class NameResolver extends NamedSpec.NameResolver[AssertionSpec]
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", visible = true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "expression", value = classOf[ExpressionAssertionSpec]),
    new JsonSubTypes.Type(name = "columns", value = classOf[ColumnsAssertionSpec]),
    new JsonSubTypes.Type(name = "schema", value = classOf[SchemaAssertionSpec]),
    new JsonSubTypes.Type(name = "sql", value = classOf[SqlAssertionSpec]),
    new JsonSubTypes.Type(name = "uniqueKey", value = classOf[UniqueKeyAssertionSpec])
))
abstract class AssertionSpec  extends NamedSpec[Assertion] {
    @JsonProperty(value="description", required = false) private var description: Option[String] = None

    override def instantiate(context: Context): Assertion

    override protected def instanceProperties(context:Context) : Assertion.Properties = {
        require(context != null)
        Assertion.Properties(
            context,
            context.namespace,
            context.project,
            name,
            kind,
            context.evaluate(labels),
            context.evaluate(description)
        )
    }
}


class AssertionSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[AssertionType]

    override def register(clazz: Class[_]): Unit =
        AssertionSpec.register(clazz.getAnnotation(classOf[AssertionType]).kind(), clazz.asInstanceOf[Class[_ <: AssertionSpec]])
}
