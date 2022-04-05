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

package com.dimajix.flowman.spec.template

import java.io.IOException

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonProperty.Access
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.DatabindContext
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver
import com.fasterxml.jackson.databind.util.StdConverter

import com.dimajix.jackson.WrappingTypeIdResolver

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.BaseTemplate
import com.dimajix.flowman.model.Category
import com.dimajix.flowman.model.Instance
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spec.assertion.AssertionSpec
import com.dimajix.flowman.spec.connection.ConnectionSpec
import com.dimajix.flowman.spec.dataset.DatasetSpec
import com.dimajix.flowman.spec.mapping.MappingSpec
import com.dimajix.flowman.spec.measure.MeasureSpec
import com.dimajix.flowman.spec.relation.RelationSpec
import com.dimajix.flowman.spec.schema.SchemaSpec
import com.dimajix.flowman.spec.target.TargetSpec
import com.dimajix.flowman.spec.test.TestSpec
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.StringType


object TemplateSpec {
    final class NameResolver extends NamedSpec.NameResolver[TemplateSpec]

    final class Parameter {
        @JsonProperty(value = "name") private var name: String = ""
        @JsonProperty(value = "description") private var description: Option[String] = None
        @JsonProperty(value = "type", required = false) private var ftype: FieldType = StringType
        @JsonProperty(value = "default", required = false) private var default: Option[String] = None

        def instantiate(context: Context): Template.Parameter = {
            Template.Parameter(
                context.evaluate(name),
                ftype,
                context.evaluate(default),
                context.evaluate(description)
            )
        }
    }
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", visible=true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "assertion", value = classOf[AssertionTemplateSpec]),
    new JsonSubTypes.Type(name = "connection", value = classOf[ConnectionTemplateSpec]),
    new JsonSubTypes.Type(name = "dataset", value = classOf[DatasetTemplateSpec]),
    new JsonSubTypes.Type(name = "mapping", value = classOf[MappingTemplateSpec]),
    new JsonSubTypes.Type(name = "measure", value = classOf[MeasureTemplateSpec]),
    new JsonSubTypes.Type(name = "relation", value = classOf[RelationTemplateSpec]),
    new JsonSubTypes.Type(name = "schema", value = classOf[SchemaTemplateSpec]),
    new JsonSubTypes.Type(name = "target", value = classOf[TargetTemplateSpec])
))
abstract class TemplateSpec extends NamedSpec[Template[_]] {
    @JsonProperty(value="kind", access=Access.WRITE_ONLY, required = true) protected var kind: String = _
    @JsonProperty(value="parameters", required=false) protected var parameters : Seq[TemplateSpec.Parameter] = Seq()

    def instantiate(context:Context, properties:Option[Template.Properties] = None) : Template[_]

    protected def instanceProperties(context:Context, properties:Option[Template.Properties]) : Template.Properties = {
        val name = context.evaluate(this.name)
        Template.Properties(
            context,
            metadata.map(_.instantiate(context, name, Category.TEMPLATE, kind)).getOrElse(Metadata(context, name, Category.TEMPLATE, kind))
        )
    }
}



class CustomTypeResolverBuilder extends com.dimajix.jackson.CustomTypeResolverBuilder {
    override protected def wrapIdResolver(resolver:TypeIdResolver, baseType: JavaType) : TypeIdResolver = {
        new CustomTypeIdResolver(resolver, baseType)
    }
}
class CustomTypeIdResolver(wrapped:TypeIdResolver, baseType:JavaType) extends WrappingTypeIdResolver(wrapped) {
    @throws[IOException]
    override def typeFromId(context: DatabindContext, id: String): JavaType = {
        if (id.startsWith("template/")) {
            if (baseType.getRawClass  eq classOf[AssertionSpec]) {
                context.constructType(classOf[AssertionTemplateInstanceSpec] )
            }
            else if (baseType.getRawClass  eq classOf[ConnectionSpec]) {
                context.constructType(classOf[ConnectionTemplateInstanceSpec] )
            }
            else if (baseType.getRawClass  eq classOf[DatasetSpec]) {
                context.constructType(classOf[DatasetTemplateInstanceSpec] )
            }
            else if (baseType.getRawClass  eq classOf[MappingSpec]) {
                context.constructType(classOf[MappingTemplateInstanceSpec] )
            }
            else if (baseType.getRawClass  eq classOf[MeasureSpec]) {
                context.constructType(classOf[MeasureTemplateInstanceSpec] )
            }
            else if (baseType.getRawClass  eq classOf[RelationSpec]) {
                context.constructType(classOf[RelationTemplateInstanceSpec] )
            }
            else if (baseType.getRawClass  eq classOf[SchemaSpec]) {
                context.constructType(classOf[SchemaTemplateInstanceSpec] )
            }
            else if (baseType.getRawClass  eq classOf[TargetSpec]) {
                context.constructType(classOf[TargetTemplateInstanceSpec] )
            }
            else {
                throw new JsonMappingException(s"Invalid template type '$id' for base type ${baseType.getRawClass.getName}")
            }
        }
        else {
            wrapped.typeFromId(context, id)
        }
    }
}
