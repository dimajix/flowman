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
import com.dimajix.flowman.documentation.ExpressionSchemaTest
import com.dimajix.flowman.documentation.ForeignKeySchemaTest
import com.dimajix.flowman.documentation.PrimaryKeySchemaTest
import com.dimajix.flowman.documentation.SchemaReference
import com.dimajix.flowman.documentation.SchemaTest
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.spec.annotation.SchemaTestType
import com.dimajix.flowman.spi.ClassAnnotationHandler


object SchemaTestSpec extends TypeRegistry[SchemaTestSpec] {
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "expression", value = classOf[ExpressionSchemaTestSpec]),
    new JsonSubTypes.Type(name = "foreignKey", value = classOf[ForeignKeySchemaTestSpec]),
    new JsonSubTypes.Type(name = "primaryKey", value = classOf[PrimaryKeySchemaTestSpec])
))
abstract class SchemaTestSpec {
    def instantiate(context: Context, parent:SchemaReference): SchemaTest
}


class SchemaTestSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[SchemaTestType]

    override def register(clazz: Class[_]): Unit =
        SchemaTestSpec.register(clazz.getAnnotation(classOf[SchemaTestType]).kind(), clazz.asInstanceOf[Class[_ <: SchemaTestSpec]])
}


class PrimaryKeySchemaTestSpec extends SchemaTestSpec {
    @JsonProperty(value="columns", required=false) private var columns:Seq[String] = Seq.empty

    override def instantiate(context: Context, parent:SchemaReference): PrimaryKeySchemaTest = PrimaryKeySchemaTest(
        Some(parent),
        columns = columns.map(context.evaluate)
    )
}
class ExpressionSchemaTestSpec extends SchemaTestSpec {
    @JsonProperty(value="expression", required=true) private var expression:String = _

    override def instantiate(context: Context, parent:SchemaReference): ExpressionSchemaTest = ExpressionSchemaTest(
        Some(parent),
        expression = context.evaluate(expression)
    )
}
class ForeignKeySchemaTestSpec extends SchemaTestSpec {
    @JsonProperty(value="mapping", required=false) private var mapping:Option[String] = None
    @JsonProperty(value="relation", required=false) private var relation:Option[String] = None
    @JsonProperty(value="columns", required=false) private var columns:Seq[String] = Seq.empty
    @JsonProperty(value="references", required=false) private var references:Seq[String] = Seq.empty

    override def instantiate(context: Context, parent:SchemaReference): ForeignKeySchemaTest = ForeignKeySchemaTest(
        Some(parent),
        columns=columns.map(context.evaluate),
        relation=context.evaluate(relation).map(RelationIdentifier(_)),
        mapping=context.evaluate(mapping).map(MappingOutputIdentifier(_)),
        references=references.map(context.evaluate)
    )
}
