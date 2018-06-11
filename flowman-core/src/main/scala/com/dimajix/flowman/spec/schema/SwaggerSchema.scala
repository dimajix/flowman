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

import scala.collection.JavaConversions._
import java.io.File
import java.net.URL
import java.nio.charset.Charset
import java.nio.file.Files

import com.fasterxml.jackson.annotation.JsonProperty
import io.swagger.models.ArrayModel
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
import io.swagger.parser.SwaggerParser
import org.apache.commons.io.IOUtils

import com.dimajix.flowman.execution.Context


class SwaggerSchema extends Schema {
    @JsonProperty(value="file", required=false) private var _file: String = _
    @JsonProperty(value="url", required=false) private var _url: String = _
    @JsonProperty(value="spec", required=false) private var _spec: String = _
    @JsonProperty(value="entity", required=false) private var _entity: String = _

    def file(implicit context: Context) : String = context.evaluate(_file)
    def url(implicit context: Context) : URL = if (_url != null && _url.nonEmpty) new URL(context.evaluate(_url)) else null
    def spec(implicit context: Context) : String = context.evaluate(_spec)
    def entity(implicit context: Context) : String = context.evaluate(_entity)

    override def description(implicit context: Context): String = {
        loadSwaggerSchema.getInfo.getDescription
    }
    override def fields(implicit context: Context): Seq[Field] = {
        val swagger = loadSwaggerSchema
        val model = Option(entity).filter(_.nonEmpty).map(e => swagger.getDefinitions()(e)).getOrElse(swagger.getDefinitions().values().head)

        if (!model.isInstanceOf[ModelImpl])
            throw new IllegalArgumentException("Root type in Swagger must be a simple model")

        fromSwaggerModel(model)
    }

    private def loadSwaggerSchema(implicit context: Context) : Swagger = {
        val file = this.file
        val url = this.url
        val spec = this.spec

        val string = if (file != null && file.nonEmpty) {
            val bytes = Files.readAllBytes(new File(file).toPath)
            new String(bytes, Charset.forName("UTF-8"))
        }
        else if (url != null) {
            IOUtils.toString(url)
        }
        else if (spec != null && spec.nonEmpty) {
            spec
        }
        else {
            throw new IllegalArgumentException("A Swagger schema needs either a 'file', 'url' or a 'spec' element")
        }

        val parser = new SwaggerParser
        parser.parse(string)
    }

    private def fromSwaggerModel(model:Model) : Seq[Field] = {
        model match {
            case composed:ComposedModel => composed.getAllOf.flatMap(fromSwaggerModel)
            //case array:ArrayModel => Seq(fromSwaggerProperty(array.getItems))
            case _ => fromSwaggerObject(model.getProperties.toSeq).fields
        }
    }

    private def fromSwaggerObject(properties:Seq[(String,Property)]) : StructType = {
        StructType(properties.sortBy(_._1).map(np => fromSwaggerProperty(np._1, np._2)))
    }

    private def fromSwaggerProperty(name:String, property:Property) : Field = {
        Field(name, fromSwaggerType(property), !property.getRequired, property.getDescription)
    }

    private def fromSwaggerType(property:Property) : FieldType = {
        property match {
            case array:ArrayProperty => ArrayType(fromSwaggerType(array.getItems))
            case _:BinaryProperty => BinaryType
            case _:BooleanProperty => BooleanType
            case _:ByteArrayProperty => BinaryType
            case _:DateProperty => DateType
            case _:DateTimeProperty => TimestampType
            case _:FloatProperty => FloatType
            case _:DoubleProperty => DoubleType
            case _:DecimalProperty => DoubleType
            case _:IntegerProperty => IntegerType
            case _:LongProperty => LongType
            case _:BaseIntegerProperty => IntegerType
            case _:MapProperty => MapType(StringType, StringType)
            case obj:ObjectProperty => fromSwaggerObject(obj.getProperties.toSeq)
            case _:StringProperty => StringType
            case _:UUIDProperty => StringType
            case _ => throw new UnsupportedOperationException(s"Swagger type of field $property not supported")
        }
    }

}
