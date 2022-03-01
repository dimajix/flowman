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
import com.dimajix.flowman.documentation.ColumnCheck
import com.dimajix.flowman.documentation.ExpressionColumnCheck
import com.dimajix.flowman.documentation.ForeignKeyColumnCheck
import com.dimajix.flowman.documentation.NotNullColumnCheck
import com.dimajix.flowman.documentation.RangeColumnCheck
import com.dimajix.flowman.documentation.UniqueColumnCheck
import com.dimajix.flowman.documentation.ValuesColumnCheck
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.spec.annotation.ColumnCheckType
import com.dimajix.flowman.spi.ClassAnnotationHandler


object ColumnCheckSpec extends TypeRegistry[ColumnCheckSpec] {
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "expression", value = classOf[ExpressionColumnCheckSpec]),
    new JsonSubTypes.Type(name = "foreignKey", value = classOf[ForeignKeyColumnCheckSpec]),
    new JsonSubTypes.Type(name = "notNull", value = classOf[NotNullColumnCheckSpec]),
    new JsonSubTypes.Type(name = "unique", value = classOf[UniqueColumnCheckSpec]),
    new JsonSubTypes.Type(name = "range", value = classOf[RangeColumnCheckSpec]),
    new JsonSubTypes.Type(name = "values", value = classOf[ValuesColumnCheckSpec])
))
abstract class ColumnCheckSpec {
    def instantiate(context: Context, parent:ColumnReference): ColumnCheck
}


class ColumnCheckSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[ColumnCheckType]

    override def register(clazz: Class[_]): Unit =
        ColumnCheckSpec.register(clazz.getAnnotation(classOf[ColumnCheckType]).kind(), clazz.asInstanceOf[Class[_ <: ColumnCheckSpec]])
}


class NotNullColumnCheckSpec extends ColumnCheckSpec {
    override def instantiate(context: Context, parent:ColumnReference): NotNullColumnCheck = NotNullColumnCheck(Some(parent))
}
class UniqueColumnCheckSpec extends ColumnCheckSpec {
    override def instantiate(context: Context, parent:ColumnReference): UniqueColumnCheck = UniqueColumnCheck(Some(parent))
}
class RangeColumnCheckSpec extends ColumnCheckSpec {
    @JsonProperty(value="lower", required=true) private var lower:String = ""
    @JsonProperty(value="upper", required=true) private var upper:String = ""

    override def instantiate(context: Context, parent:ColumnReference): RangeColumnCheck = RangeColumnCheck(
        Some(parent),
        None,
        context.evaluate(lower),
        context.evaluate(upper)
    )
}
class ValuesColumnCheckSpec extends ColumnCheckSpec {
    @JsonProperty(value="values", required=false) private var values:Seq[String] = Seq()

    override def instantiate(context: Context, parent:ColumnReference): ValuesColumnCheck = ValuesColumnCheck(
        Some(parent),
        values=values.map(context.evaluate)
    )
}
class ExpressionColumnCheckSpec extends ColumnCheckSpec {
    @JsonProperty(value="expression", required=true) private var expression:String = _

    override def instantiate(context: Context, parent:ColumnReference): ExpressionColumnCheck = ExpressionColumnCheck(
        Some(parent),
        expression=context.evaluate(expression)
    )
}
class ForeignKeyColumnCheckSpec extends ColumnCheckSpec {
    @JsonProperty(value="mapping", required=false) private var mapping:Option[String] = None
    @JsonProperty(value="relation", required=false) private var relation:Option[String] = None
    @JsonProperty(value="column", required=false) private var column:Option[String] = None

    override def instantiate(context: Context, parent:ColumnReference): ForeignKeyColumnCheck = ForeignKeyColumnCheck(
        Some(parent),
        relation=context.evaluate(relation).map(RelationIdentifier(_)),
        mapping=context.evaluate(mapping).map(MappingOutputIdentifier(_)),
        column=context.evaluate(column)
    )
}
