/*
 * Copyright 2018 Kaya Kupferschmidt
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

import scala.collection.JavaConverters._

import io.swagger.v3.core.util.Json
import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.media.ArraySchema
import io.swagger.v3.oas.models.media.BinarySchema
import io.swagger.v3.oas.models.media.BooleanSchema
import io.swagger.v3.oas.models.media.ByteArraySchema
import io.swagger.v3.oas.models.media.ComposedSchema
import io.swagger.v3.oas.models.media.DateSchema
import io.swagger.v3.oas.models.media.DateTimeSchema
import io.swagger.v3.oas.models.media.IntegerSchema
import io.swagger.v3.oas.models.media.MapSchema
import io.swagger.v3.oas.models.media.NumberSchema
import io.swagger.v3.oas.models.media.ObjectSchema
import io.swagger.v3.oas.models.media.Schema
import io.swagger.v3.oas.models.media.StringSchema
import io.swagger.v3.oas.models.media.UUIDSchema
import io.swagger.v3.parser.util.DeserializationUtils
import io.swagger.v3.parser.util.OpenAPIDeserializer

import com.dimajix.flowman.types.ArrayType
import com.dimajix.flowman.types.BinaryType
import com.dimajix.flowman.types.BooleanType
import com.dimajix.flowman.types.ByteType
import com.dimajix.flowman.types.CharType
import com.dimajix.flowman.types.DateType
import com.dimajix.flowman.types.DecimalType
import com.dimajix.flowman.types.DoubleType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.FloatType
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.LongType
import com.dimajix.flowman.types.MapType
import com.dimajix.flowman.types.ShortType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.types.TimestampType
import com.dimajix.flowman.types.VarcharType


object OpenApiSchemaUtils {
    /**
      * Convert an entity of a OpenAPI schema into a Flowman schema. Optionally mark all fields as optional.
      *
      * @param schema
      * @param entity
      * @param nullable
      * @return
      */
    def fromOpenApi(schema:String, entity:Option[String]=None, nullable:Boolean=true) : Seq[Field] = {
        val swagger = parse(schema)
        fromOpenApi(swagger, entity, nullable)
    }

    /**
      * Convert an entity of a OpenAPI schema into a Flowman schema. Optionally mark all fields as optional.
      *
      * @param api
      * @param entity
      * @param nullable
      * @return
      */
    def fromOpenApi(api:OpenAPI, entity:Option[String], nullable:Boolean) : Seq[Field] = {
        val schemas = Option(api.getComponents())
            .flatMap(e => Option(e.getSchemas))
            .map(_.asScala)
            .getOrElse(Map.empty[String,Schema[_]])
        val model = entity.filter(_.nonEmpty)
            .map(e => schemas.getOrElse(e, throw new IllegalArgumentException(s"Entity $e not found in schema")))
            .getOrElse(schemas.values.head)
        fromOpenApi(model, nullable)
    }

    /**
      * Convert an entity of a OpenAPI schema into a Flowman schema. Optionally mark all fields as optional.
      *
      * @param model
      * @param nullable
      * @return
      */
    def fromOpenApi(model:Schema[_], nullable:Boolean) : Seq[Field] = {
        def fromOpenApiRec(model:Schema[_], nullable:Boolean=true) : Seq[Field] = {
            model match {
                case composed: ComposedSchema => composed.getAllOf.asScala.flatMap(m => fromOpenApiRec(m, nullable))
                //case array:ArrayModel => Seq(fromSwaggerProperty(array.getItems))
                case _ => fromOpenApiObject(model.getProperties.asScala.toSeq, "", Option(model.getRequired).toSeq.flatMap(_.asScala).toSet, nullable).fields
            }
        }

        fromOpenApiRec(model, nullable)
    }

    /**
      * Parse a string as a OpenAPI schema. This will also fix some incompatible representations (nested allOf)
      *
      * @param data
      * @return
      */
    def parse(data: String): OpenAPI = {
        val rootNode = if (data.trim.startsWith("{")) {
            val mapper = Json.mapper
            mapper.readTree(data)
        }
        else {
            try {
                val opts = classOf[DeserializationUtils].getMethod("getOptions").invoke(null)
                Class.forName("io.swagger.v3.parser.util.DeserializationUtils$Options").getMethod("setMaxYamlCodePoints", classOf[Integer]).invoke(opts, Integer.valueOf(128 * 1024 * 1024))
            }
            catch {
                case _:NoSuchMethodException =>
                case _:ClassNotFoundException =>
            }

            DeserializationUtils.readYamlTree(data)
        }

        val result = new OpenAPIDeserializer().deserialize(rootNode)
        val convertValue = result.getOpenAPI
        convertValue
    }

    private def fromOpenApiObject(properties:Seq[(String,Schema[_])], prefix:String, required:Set[String], nullable:Boolean) : StructType = {
        StructType(properties.map { np =>
            fromOpenApiProperty(np._1, np._2, prefix, required, nullable)
        })
    }

    private def fromOpenApiProperty(name:String, property:Schema[_], prefix:String, required:Set[String], nullable:Boolean) : Field = {
        Field(
            name,
            fromOpenApiType(property, prefix + name, nullable),
            nullable  || !required.contains(name),
            Option(property.getDescription),
            None, // default value
            None, // size
            Option(property.getFormat)
        )
    }

    private def fromOpenApiType(property:Schema[_], fqName:String, nullable:Boolean) : FieldType = {
        def required(schema:Schema[_]) : Set[String] = {
            Option(schema.getRequired).toSeq.flatMap(_.asScala).toSet
        }
        property match {
            case array:ArraySchema => ArrayType(fromOpenApiType(array.getItems, fqName + ".items", nullable))
            case _:BinarySchema => BinaryType
            case _:BooleanSchema => BooleanType
            case _:ByteArraySchema => BinaryType
            case _:DateSchema => DateType
            case _:DateTimeSchema => TimestampType
            case d:NumberSchema if d.getFormat == "float" => FloatType
            case d:NumberSchema if d.getFormat == "double" => DoubleType
            case d:NumberSchema =>
                val scale = if (d.getMultipleOf != null) d.getMultipleOf.scale() else DecimalType.USER_DEFAULT.scale
                val precision = if (d.getMaximum != null) d.getMaximum.precision() else DecimalType.MAX_PRECISION - scale
                DecimalType(precision + scale, scale)
            case i:IntegerSchema =>
                i.getFormat match {
                    case "int8" => ByteType
                    case "int16" => ShortType
                    case "int32" => IntegerType
                    case "int64" => LongType
                    case _ => IntegerType
                }
            case _:MapSchema => MapType(StringType, StringType)
            case s:StringSchema =>
                val minLength = Option(s.getMinLength).map(_.intValue())
                val maxLength = Option(s.getMaxLength).map(_.intValue())
                (minLength, maxLength) match {
                    case (Some(l1),Some(l2)) if l1==l2 => CharType(l1)
                    case (_,Some(l)) => VarcharType(l)
                    case (_,_) => StringType
                }
            case _:UUIDSchema => StringType
            case c:ComposedSchema => StructType(c.getAllOf.asScala.flatMap(s => fromOpenApiType(s, fqName + ".", nullable).asInstanceOf[StructType].fields))
            case obj:ObjectSchema => fromOpenApiObject(obj.getProperties.asScala.toSeq, fqName + ".", required(obj), nullable)
            case s:Schema[_] if s.getEnum != null => StringType // enums
            case s:Schema[_] if s.getProperties != null => fromOpenApiObject(s.getProperties.asScala.toSeq, fqName + ".", required(s), nullable)
            case _ => throw new UnsupportedOperationException(s"OpenAPI type $property of field $fqName not supported")
        }
    }
}
