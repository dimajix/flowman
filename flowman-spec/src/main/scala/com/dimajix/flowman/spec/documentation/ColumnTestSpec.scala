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

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.common.TypeRegistry
import com.dimajix.flowman.documentation.ColumnReference
import com.dimajix.flowman.documentation.ColumnTest
import com.dimajix.flowman.documentation.ExpressionColumnTest
import com.dimajix.flowman.documentation.ForeignKeyColumnTest
import com.dimajix.flowman.documentation.NotNullColumnTest
import com.dimajix.flowman.documentation.RangeColumnTest
import com.dimajix.flowman.documentation.UniqueColumnTest
import com.dimajix.flowman.documentation.ValuesColumnTest
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.spec.annotation.ColumnTestType
import com.dimajix.flowman.spi.ClassAnnotationHandler


object ColumnTestSpec extends TypeRegistry[ColumnTestSpec] {
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "expression", value = classOf[ExpressionColumnTestSpec]),
    new JsonSubTypes.Type(name = "foreignKey", value = classOf[ForeignKeyColumnTestSpec]),
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
    override def instantiate(context: Context, parent:ColumnReference): NotNullColumnTest = NotNullColumnTest(Some(parent))
}
class UniqueColumnTestSpec extends ColumnTestSpec {
    override def instantiate(context: Context, parent:ColumnReference): UniqueColumnTest = UniqueColumnTest(Some(parent))
}
class RangeColumnTestSpec extends ColumnTestSpec {
    @JsonProperty(value="lower", required=true) private var lower:String = ""
    @JsonProperty(value="upper", required=true) private var upper:String = ""

    override def instantiate(context: Context, parent:ColumnReference): RangeColumnTest = RangeColumnTest(
        Some(parent),
        None,
        context.evaluate(lower),
        context.evaluate(upper)
    )
}
class ValuesColumnTestSpec extends ColumnTestSpec {
    @JsonProperty(value="values", required=false) private var values:Seq[String] = Seq()

    override def instantiate(context: Context, parent:ColumnReference): ValuesColumnTest = ValuesColumnTest(
        Some(parent),
        values=values.map(context.evaluate)
    )
}
class ExpressionColumnTestSpec extends ColumnTestSpec {
    @JsonProperty(value="expression", required=true) private var expression:String = _

    override def instantiate(context: Context, parent:ColumnReference): ExpressionColumnTest = ExpressionColumnTest(
        Some(parent),
        expression=context.evaluate(expression)
    )
}
class ForeignKeyColumnTestSpec extends ColumnTestSpec {
    @JsonProperty(value="mapping", required=false) private var mapping:Option[String] = None
    @JsonProperty(value="relation", required=false) private var relation:Option[String] = None
    @JsonProperty(value="column", required=false) private var column:Option[String] = None

    override def instantiate(context: Context, parent:ColumnReference): ForeignKeyColumnTest = ForeignKeyColumnTest(
        Some(parent),
        relation=context.evaluate(relation).map(RelationIdentifier(_)),
        mapping=context.evaluate(mapping).map(MappingOutputIdentifier(_)),
        column=context.evaluate(column)
    )
}
