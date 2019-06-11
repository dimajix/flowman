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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.AbstractInstance
import com.dimajix.flowman.spec.Instance
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.Spec
import com.dimajix.flowman.spi.TypeRegistry
import com.dimajix.flowman.types.ArrayType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.MapType
import com.dimajix.flowman.types.StructType


object Schema {
    object Properties {
        def apply(context:Context, name:String="", kind:String="") : Properties = {
            Properties(
                context,
                context.namespace,
                context.project,
                name,
                kind,
                Map()
            )
        }
    }
    case class Properties(
        context:Context,
        namespace:Namespace,
        project:Project,
        name: String,
        kind: String,
        labels: Map[String, String]
    ) extends Instance.Properties {
    }
}


/**
  * Interface class for declaring relations (for sources and sinks) as part of a model
  */
abstract class Schema extends AbstractInstance {
    /**
      * Returns the category of the resource
      *
      * @return
      */
    override def category: String = "schema"

    /**
      * Returns the description of the schema
 *
      * @return
      */
    def description : Option[String]

    /**
      * Returns the list of all fields of the schema
      * @return
      */
    def fields : Seq[Field]

    /**
      * Returns the list of primary keys. Can be empty of no PK is available
      * @return
      */
    def primaryKey : Seq[String]

    /**
      * Returns a Spark schema for this schema
      * @return
      */
    def sparkSchema : org.apache.spark.sql.types.StructType = {
        org.apache.spark.sql.types.StructType(fields.map(_.sparkField))
    }

    /**
      * Provides a human readable string representation of the schema
      */
    def printTree : Unit = {
        println(treeString)
    }
    /**
      * Provides a human readable string representation of the schema
      */
    def treeString : String = {
        val builder = new StringBuilder
        builder.append("root\n")
        val prefix = " |"
        fields.foreach(field => buildTreeString(field, prefix, builder))

        builder.toString()
    }

    private def buildTreeString(field:Field, prefix:String, builder:StringBuilder) : Unit = {
        builder.append(s"$prefix-- ${field.name}: ${field.typeName} (nullable = ${field.nullable})\n")
        buildTreeString(field.ftype, s"$prefix    |", builder)
    }

    private def buildTreeString(ftype:FieldType, prefix:String, builder:StringBuilder) : Unit = {
        ftype match {
            case struct:StructType =>
                struct.fields.foreach(field => buildTreeString(field, prefix, builder))
            case map:MapType =>
                builder.append(s"$prefix-- key: ${map.keyType.typeName}\n")
                builder.append(s"$prefix-- value: ${map.valueType.typeName} " +
                    s"(containsNull = ${map.containsNull})\n")
                buildTreeString(map.keyType, s"$prefix    |", builder)
                buildTreeString(map.valueType, s"$prefix    |", builder)
            case array:ArrayType =>
                builder.append(
                    s"$prefix-- element: ${array.elementType.typeName} (containsNull = ${array.containsNull})\n")
                buildTreeString(array.elementType, s"$prefix    |", builder)
            case _ =>
        }
    }
}



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
    new JsonSubTypes.Type(name = "swagger", value = classOf[SwaggerSchemaSpec])
))
abstract class SchemaSpec extends Spec[Schema] {
    override def instantiate(context:Context) : Schema
}
