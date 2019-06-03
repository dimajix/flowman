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

import java.io.IOException
import java.net.URL

import scala.collection.JavaConversions._

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import io.swagger.models.ComposedModel
import io.swagger.models.Model
import io.swagger.models.ModelImpl
import io.swagger.models.Swagger
import io.swagger.models.properties.ArrayProperty
import io.swagger.models.properties.BaseIntegerProperty
import io.swagger.models.properties.BinaryProperty
import io.swagger.models.properties.BooleanProperty
import io.swagger.models.properties.ByteArrayProperty
import io.swagger.models.properties.DateProperty
import io.swagger.models.properties.DateTimeProperty
import io.swagger.models.properties.DecimalProperty
import io.swagger.models.properties.DoubleProperty
import io.swagger.models.properties.FloatProperty
import io.swagger.models.properties.IntegerProperty
import io.swagger.models.properties.LongProperty
import io.swagger.models.properties.MapProperty
import io.swagger.models.properties.ObjectProperty
import io.swagger.models.properties.Property
import io.swagger.models.properties.StringProperty
import io.swagger.models.properties.UUIDProperty
import io.swagger.parser.util.DeserializationUtils
import io.swagger.parser.util.SwaggerDeserializer
import io.swagger.util.Json
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.hadoop.File
import com.dimajix.flowman.spec.Instance
import com.dimajix.flowman.spec.schema.ExternalSchema.CachedSchema
import com.dimajix.flowman.types.ArrayType
import com.dimajix.flowman.types.BinaryType
import com.dimajix.flowman.types.BooleanType
import com.dimajix.flowman.types.DateType
import com.dimajix.flowman.types.DecimalType
import com.dimajix.flowman.types.DoubleType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.FloatType
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.LongType
import com.dimajix.flowman.types.MapType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.types.TimestampType


/**
  * Schema implementation for reading Swagger / OpenAPI schemas. This implementation will preserve the ordering of
  * fields.
  */
case class SwaggerSchema(
    instanceProperties:Schema.Properties,
    override val file: File,
    override val url: URL,
    override val spec: String,
    entity: String,
    nullable: Boolean
) extends ExternalSchema {
    protected override val logger = LoggerFactory.getLogger(classOf[SwaggerSchema])

    /**
      * Returns the list of all fields of the schema
      * @return
      */
    protected override def loadSchema : CachedSchema = {
        val string = loadSchemaSpec
        val swagger = convertToSwagger(string)
        val model = Option(entity).filter(_.nonEmpty).map(e => swagger.getDefinitions()(e)).getOrElse(swagger.getDefinitions().values().head)

        if (!model.isInstanceOf[ModelImpl] && !model.isInstanceOf[ComposedModel])
            throw new IllegalArgumentException("Root type in Swagger must be a simple model or composed model")

        CachedSchema(
            fromSwaggerModel(model, nullable),
            Option(swagger.getInfo).map(_.getDescription).orNull
        )
    }

    @throws[IOException]
    private def convertToSwagger(data: String): Swagger = {
        val rootNode = if (data.trim.startsWith("{")) {
            val mapper = Json.mapper
            mapper.readTree(data)
        }
        else {
            DeserializationUtils.readYamlTree(data)
        }

        // Fix nested "allOf" nodes, which have to be in "definitions->[Entity]->[Definition]"
        val possiblyUnparsableNodes = rootNode.path("definitions").elements().flatMap(_.elements())
        possiblyUnparsableNodes.foreach(replaceAllOf)

        val result = new SwaggerDeserializer().deserialize(rootNode)
        val convertValue = result.getSwagger
        convertValue
    }

    /**
      * This helper method transforms the Json tree such that "allOf" inline elements will be replaced by an
      * adequate object definition, because Swagger will not parse inline allOf elements correctly
      * @param jsonNode
      */
    private def replaceAllOf(jsonNode: JsonNode) : Unit = {
        jsonNode match {
            case obj:ObjectNode =>
                if (obj.get("allOf") != null) {
                    val children = obj.get("allOf").elements().toSeq
                    val required = children.flatMap(c => Option(c.get("required")).toSeq.flatMap(_.elements()))
                    val properties = children.flatMap(c => Option(c.get("properties")).toSeq.flatMap(_.fields()))
                    val desc = children.flatMap(c => Option(c.get("description"))).headOption
                    obj.without("allOf")
                    obj.set("type", TextNode.valueOf("object"))
                    obj.withArray("required").addAll(required)
                    properties.foreach(x => obj.`with`("properties").set(x.getKey, x.getValue))
                    desc.foreach(d => obj.set("description", d))
                }
            case _:JsonNode =>
        }
        jsonNode.elements().foreach(replaceAllOf)
    }

    private def fromSwaggerModel(model:Model, nullable:Boolean) : Seq[Field] = {
        model match {
            case composed:ComposedModel => composed.getAllOf.flatMap(m => fromSwaggerModel(m, nullable))
            //case array:ArrayModel => Seq(fromSwaggerProperty(array.getItems))
            case _ => fromSwaggerObject(model.getProperties.toSeq, "", nullable).fields
        }
    }

    private def fromSwaggerObject(properties:Seq[(String,Property)], prefix:String, nullable:Boolean) : StructType = {
        StructType(properties.map(np => fromSwaggerProperty(np._1, np._2, prefix, nullable)))
    }

    private def fromSwaggerProperty(name:String, property:Property, prefix:String, nullable:Boolean) : Field = {
        Field(name, fromSwaggerType(property, prefix + name, nullable), nullable || !property.getRequired, property.getDescription)
    }

    private def fromSwaggerType(property:Property, fqName:String, nullable:Boolean) : FieldType = {
        property match {
            case array:ArrayProperty => ArrayType(fromSwaggerType(array.getItems, fqName + ".items", nullable))
            case _:BinaryProperty => BinaryType
            case _:BooleanProperty => BooleanType
            case _:ByteArrayProperty => BinaryType
            case _:DateProperty => DateType
            case _:DateTimeProperty => TimestampType
            case _:FloatProperty => FloatType
            case _:DoubleProperty => DoubleType
            case d:DecimalProperty =>
                val scale = if (d.getMultipleOf != null) d.getMultipleOf.scale() else DecimalType.USER_DEFAULT.scale
                val precision = if (d.getMaximum != null) d.getMaximum.precision() else DecimalType.USER_DEFAULT.precision - scale
                DecimalType(precision + scale, scale)
            case _:IntegerProperty => IntegerType
            case _:LongProperty => LongType
            case _:BaseIntegerProperty => IntegerType
            case _:MapProperty => MapType(StringType, StringType)
            case obj:ObjectProperty => fromSwaggerObject(obj.getProperties.toSeq, fqName + ".", nullable)
            case _:StringProperty => StringType
            case _:UUIDProperty => StringType
            case _ => throw new UnsupportedOperationException(s"Swagger type $property of field $fqName not supported")
        }
    }
}



class SwaggerSchemaSpec extends ExternalSchemaSpec {
    @JsonProperty(value="entity", required=false) private var entity: String = _
    @JsonProperty(value="nullable", required=false) private var nullable: String = "false"

    /**
      * Creates the instance of the specified Schema with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): SwaggerSchema = {
        SwaggerSchema(
            Schema.Properties(context),
            Option(file).map(context.evaluate).filter(_.nonEmpty).map(context.fs.file).orNull,
            Option(url).map(context.evaluate).filter(_.nonEmpty).map(u => new URL(u)).orNull,
            context.evaluate(spec),
            context.evaluate(entity),
            context.evaluate(nullable).toBoolean
        )
    }
}
