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

package com.dimajix.flowman.types

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException
import org.apache.spark.sql.types.DataType


@JsonDeserialize(using=classOf[MapTypeDeserializer])
final case class MapType(
      @JsonProperty(value="keyType") keyType:FieldType,
      @JsonProperty(value="valueType") valueType:FieldType,
      @JsonProperty(value="containsNull") containsNull:Boolean = true
) extends ContainerType {
    /**
     * The Spark type to use
     * @return
     */
    override def sparkType : org.apache.spark.sql.types.MapType = {
        org.apache.spark.sql.types.MapType(keyType.sparkType, valueType.sparkType, containsNull)
    }

    /**
     * The Spark type to use
     * @return
     */
    override def catalogType : org.apache.spark.sql.types.MapType = {
        org.apache.spark.sql.types.MapType(keyType.catalogType, valueType.catalogType, containsNull)
    }

    override def parse(value:String, granularity:Option[String]=None) : Any = ???
    override def interpolate(value: FieldValue, granularity:Option[String]=None) : Iterable[Any] = ???
}

/*
 * This is a workaround for Jackson versions which do not use default values
 */
private class MapTypeDeserializer(vc:Class[_]) extends StdDeserializer[MapType](vc) {
    import java.io.IOException

    def this() = this(null)

    @throws[IOException]
    @throws[JsonProcessingException]
    def deserialize(jp: JsonParser, ctxt: DeserializationContext): MapType = {
        var currentToken: JsonToken = jp.nextValue()
        var keyType: FieldType = null
        var valueType: FieldType = null
        var containsNull: Boolean = true
        while(currentToken != JsonToken.END_OBJECT) {
            jp.getCurrentName match {
                case "keyType" => keyType = jp.readValueAs(classOf[FieldType])
                case "valueType" => valueType = jp.readValueAs(classOf[FieldType])
                case "containsNull" => containsNull = jp.getBooleanValue
                case _ => throw UnrecognizedPropertyException.from(jp, classOf[MapType], jp.getCurrentName, null)
            }
            currentToken = jp.nextValue()
        }
        MapType(keyType, valueType, containsNull)
    }
}
