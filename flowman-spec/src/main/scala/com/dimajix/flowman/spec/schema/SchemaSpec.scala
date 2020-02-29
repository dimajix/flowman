/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.common.TypeRegistry
import com.dimajix.flowman.annotation.SchemaType
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.Spec
import com.dimajix.flowman.spi.ClassAnnotationHandler


object SchemaSpec extends TypeRegistry[SchemaSpec] {
}

/**
  * Interface class for declaring relations (for sources and sinks) as part of a model
  */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", defaultImpl = classOf[EmbeddedSchemaSpec])
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "inline", value = classOf[EmbeddedSchemaSpec]),
    new JsonSubTypes.Type(name = "embedded", value = classOf[EmbeddedSchemaSpec]),
    new JsonSubTypes.Type(name = "avro", value = classOf[AvroSchemaSpec]),
    new JsonSubTypes.Type(name = "json", value = classOf[JsonSchemaSpec]),
    new JsonSubTypes.Type(name = "mapping", value = classOf[MappingSchemaSpec]),
    new JsonSubTypes.Type(name = "relation", value = classOf[RelationSchemaSpec]),
    new JsonSubTypes.Type(name = "spark", value = classOf[SparkSchemaSpec]),
    new JsonSubTypes.Type(name = "swagger", value = classOf[SwaggerSchemaSpec]),
    new JsonSubTypes.Type(name = "union", value = classOf[UnionSchemaSpec])
))
abstract class SchemaSpec extends Spec[Schema] {
    override def instantiate(context:Context) : Schema
}



class SchemaSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[SchemaType]

    override def register(clazz: Class[_]): Unit =
        SchemaSpec.register(clazz.getAnnotation(classOf[SchemaType]).kind(), clazz.asInstanceOf[Class[_ <: SchemaSpec]])
}
