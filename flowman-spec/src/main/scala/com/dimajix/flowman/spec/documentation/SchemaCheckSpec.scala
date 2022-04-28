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
import com.dimajix.flowman.documentation.ExpressionSchemaCheck
import com.dimajix.flowman.documentation.ForeignKeySchemaCheck
import com.dimajix.flowman.documentation.PrimaryKeySchemaCheck
import com.dimajix.flowman.documentation.SchemaReference
import com.dimajix.flowman.documentation.SchemaCheck
import com.dimajix.flowman.documentation.SqlSchemaCheck
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.spec.annotation.SchemaCheckType
import com.dimajix.flowman.spi.ClassAnnotationHandler


object SchemaCheckSpec extends TypeRegistry[SchemaCheckSpec] {
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "expression", value = classOf[ExpressionSchemaCheckSpec]),
    new JsonSubTypes.Type(name = "foreignKey", value = classOf[ForeignKeySchemaCheckSpec]),
    new JsonSubTypes.Type(name = "primaryKey", value = classOf[PrimaryKeySchemaCheckSpec]),
    new JsonSubTypes.Type(name = "sql", value = classOf[SqlSchemaCheckSpec])
))
abstract class SchemaCheckSpec {
    def instantiate(context: Context, parent:SchemaReference): SchemaCheck
}


class SchemaCheckSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[SchemaCheckType]

    override def register(clazz: Class[_]): Unit =
        SchemaCheckSpec.register(clazz.getAnnotation(classOf[SchemaCheckType]).kind(), clazz.asInstanceOf[Class[_ <: SchemaCheckSpec]])
}


class PrimaryKeySchemaCheckSpec extends SchemaCheckSpec {
    @JsonProperty(value="columns", required=false) private var columns:Seq[String] = Seq.empty
    @JsonProperty(value="filter", required=false) private var filter:Option[String] = None

    override def instantiate(context: Context, parent:SchemaReference): PrimaryKeySchemaCheck = PrimaryKeySchemaCheck(
        Some(parent),
        columns = columns.map(context.evaluate),
        filter = context.evaluate(filter)
    )
}
class ExpressionSchemaCheckSpec extends SchemaCheckSpec {
    @JsonProperty(value="expression", required=true) private var expression:String = _
    @JsonProperty(value="filter", required=false) private var filter:Option[String] = None

    override def instantiate(context: Context, parent:SchemaReference): ExpressionSchemaCheck = ExpressionSchemaCheck(
        Some(parent),
        expression = context.evaluate(expression),
        filter = context.evaluate(filter)
    )
}
class ForeignKeySchemaCheckSpec extends SchemaCheckSpec {
    @JsonProperty(value="mapping", required=false) private var mapping:Option[String] = None
    @JsonProperty(value="relation", required=false) private var relation:Option[String] = None
    @JsonProperty(value="columns", required=false) private var columns:Seq[String] = Seq.empty
    @JsonProperty(value="references", required=false) private var references:Seq[String] = Seq.empty
    @JsonProperty(value="filter", required=false) private var filter:Option[String] = None

    override def instantiate(context: Context, parent:SchemaReference): ForeignKeySchemaCheck = ForeignKeySchemaCheck(
        Some(parent),
        columns = columns.map(context.evaluate),
        relation = context.evaluate(relation).map(RelationIdentifier(_)),
        mapping = context.evaluate(mapping).map(MappingOutputIdentifier(_)),
        references = references.map(context.evaluate),
        filter = context.evaluate(filter)
    )
}
class SqlSchemaCheckSpec extends SchemaCheckSpec {
    @JsonProperty(value="query", required=true) private var query:String = _
    @JsonProperty(value="filter", required=false) private var filter:Option[String] = None

    override def instantiate(context: Context, parent:SchemaReference): SqlSchemaCheck = SqlSchemaCheck(
        Some(parent),
        query = context.evaluate(query),
        filter = context.evaluate(filter)
    )
}
