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
import com.dimajix.flowman.documentation.ColumnReference
import com.dimajix.flowman.documentation.ColumnTest
import com.dimajix.flowman.documentation.NotNullColumnTest
import com.dimajix.flowman.documentation.RangeColumnTest
import com.dimajix.flowman.documentation.UniqueColumnTest
import com.dimajix.flowman.documentation.ValuesColumnTest
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.annotation.ColumnTestType
import com.dimajix.flowman.spi.ClassAnnotationHandler


object ColumnTestSpec extends TypeRegistry[ColumnTestSpec] {
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "notNull", value = classOf[NotNullColumnTestSpec]),
    new JsonSubTypes.Type(name = "unique", value = classOf[UniqueColumnTestSpec]),
    new JsonSubTypes.Type(name = "range", value = classOf[RangeColumnTestSpec]),
    new JsonSubTypes.Type(name = "values", value = classOf[ValuesColumnTestSpec])
))
abstract class ColumnTestSpec {
    def instantiate(context: Context, parent:ColumnReference): ColumnTest
}


class ColumnTestSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[ColumnTestType]

    override def register(clazz: Class[_]): Unit =
        ColumnTestSpec.register(clazz.getAnnotation(classOf[ColumnTestType]).kind(), clazz.asInstanceOf[Class[_ <: ColumnTestSpec]])
}


class NotNullColumnTestSpec extends ColumnTestSpec {
    override def instantiate(context: Context, parent:ColumnReference): ColumnTest = NotNullColumnTest(Some(parent))
}
class UniqueColumnTestSpec extends ColumnTestSpec {
    override def instantiate(context: Context, parent:ColumnReference): ColumnTest = UniqueColumnTest(Some(parent))
}
class RangeColumnTestSpec extends ColumnTestSpec {
    override def instantiate(context: Context, parent:ColumnReference): ColumnTest = RangeColumnTest(Some(parent))
}
class ValuesColumnTestSpec extends ColumnTestSpec {
    override def instantiate(context: Context, parent:ColumnReference): ColumnTest = ValuesColumnTest(Some(parent))
}
