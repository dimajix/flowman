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

package com.dimajix.flowman.spec.schema

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonProperty.Access
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.annotation.JsonTypeResolver

import com.dimajix.common.TypeRegistry
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Category
import com.dimajix.flowman.model.Dataset
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.Spec
import com.dimajix.flowman.spec.annotation.SchemaType
import com.dimajix.flowman.spec.template.CustomTypeResolverBuilder
import com.dimajix.flowman.spi.ClassAnnotationHandler


object SchemaSpec extends TypeRegistry[SchemaSpec] {
}

/**
  * Interface class for declaring relations (for sources and sinks) as part of a model
  */
@JsonTypeResolver(classOf[CustomTypeResolverBuilder])
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property="kind", defaultImpl=classOf[InlineSchemaSpec], visible=true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "avro", value = classOf[AvroSchemaSpec]),
    new JsonSubTypes.Type(name = "inline", value = classOf[InlineSchemaSpec]),
    new JsonSubTypes.Type(name = "mapping", value = classOf[MappingSchemaSpec]),
    new JsonSubTypes.Type(name = "relation", value = classOf[RelationSchemaSpec]),
    new JsonSubTypes.Type(name = "spark", value = classOf[SparkSchemaSpec]),
    new JsonSubTypes.Type(name = "union", value = classOf[UnionSchemaSpec])
))
abstract class SchemaSpec extends Spec[Schema] {
    @JsonProperty(value="kind", access=Access.WRITE_ONLY, required=false, defaultValue="inline") protected var kind: String = "inline"

    override def instantiate(context:Context, properties:Option[Schema.Properties] = None) : Schema

    /**
     * Returns a set of common properties
     * @param context
     * @return
     */
    protected def instanceProperties(context:Context, name:String) : Schema.Properties = {
        require(context != null)
        Schema.Properties(
            context,
            Metadata(context, name, Category.SCHEMA, kind)
        )
    }
}


class SchemaSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[SchemaType]

    override def register(clazz: Class[_]): Unit =
        SchemaSpec.register(clazz.getAnnotation(classOf[SchemaType]).kind(), clazz.asInstanceOf[Class[_ <: SchemaSpec]])
}
