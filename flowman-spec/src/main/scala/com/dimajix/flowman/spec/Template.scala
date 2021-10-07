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

package com.dimajix.flowman.spec

import java.io.IOException
import java.util

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.DatabindContext
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.cfg.MapperConfig
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder
import com.fasterxml.jackson.databind.jsontype.impl.TypeNameIdResolver
import com.fasterxml.jackson.databind.util.StdConverter

import com.dimajix.flowman.model.BaseTemplate
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.spec.relation.RelationSpec
import com.dimajix.flowman.spec.relation.RelationTemplateInstanceSpec
import com.dimajix.flowman.spec.relation.RelationTemplateSpec
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.StringType


object TemplateSpec {
    final class NameResolver extends StdConverter[Map[String, TemplateSpec[_]], Map[String, TemplateSpec[_]]] {
        override def convert(value: Map[String, TemplateSpec[_]]): Map[String, TemplateSpec[_]] = {
            value.foreach(kv => kv._2.name = kv._1)
            value
        }
    }

    final class Parameter {
        @JsonProperty(value = "name") private var name: String = ""
        @JsonProperty(value = "description") private var description: Option[String] = None
        @JsonProperty(value = "type", required = false) private var ftype: FieldType = StringType
        @JsonProperty(value = "default", required = false) private var default: Option[String] = None

        def instantiate(): Template.Parameter = {
            Template.Parameter(
                name,
                ftype,
                default,
                description
            )
        }
    }
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", visible=true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "relation", value = classOf[RelationTemplateSpec])
))
abstract class TemplateSpec[T] extends BaseTemplate[T] {
    @JsonIgnore var name:String = ""

    override def parameters: Seq[Template.Parameter] = _parameters.map(_.instantiate())

    @JsonProperty(value="kind", required = true) protected var kind: String = _
    @JsonProperty(value="labels", required=false) protected var labels:Map[String,String] = Map()
    @JsonProperty(value="parameters", required=false) protected var _parameters : Seq[TemplateSpec.Parameter] = Seq()
}



class CustomTypeResolverBuilder extends StdTypeResolverBuilder {
    override protected def idResolver(config: MapperConfig[_], baseType: JavaType, subtypeValidator: PolymorphicTypeValidator, subtypes: util.Collection[NamedType], forSer: Boolean, forDeser: Boolean) : TypeIdResolver = {
        val resolver = TypeNameIdResolver.construct(config, baseType, subtypes, forSer, forDeser)
        new CustomTypeIdResolver(resolver, baseType)
    }
}
class CustomTypeIdResolver(wrapped:TypeIdResolver, baseType:JavaType) extends TypeIdResolver {
    override def init(baseType: JavaType): Unit = wrapped.init(baseType)
    override def idFromValue(value: Any): String = wrapped.idFromValue(value)
    override def idFromValueAndType(value: Any, suggestedType: Class[_]): String = wrapped.idFromValueAndType(value, suggestedType)
    override def idFromBaseType(): String = wrapped.idFromBaseType()
    @throws[IOException]
    override def typeFromId(context: DatabindContext, id: String): JavaType = {
        if (id.startsWith("template/")) {
            if (baseType.getRawClass  eq classOf[RelationSpec]) {
                context.constructType(classOf[RelationTemplateInstanceSpec] )
            }
            else {
                throw new InvalidTypeIdException(null, "Invalid template type", baseType, id)
            }
        }
        else {
            wrapped.typeFromId(context, id)
        }
    }
    override def getDescForKnownTypeIds: String = wrapped.getDescForKnownTypeIds
    override def getMechanism: JsonTypeInfo.Id = wrapped.getMechanism
}
