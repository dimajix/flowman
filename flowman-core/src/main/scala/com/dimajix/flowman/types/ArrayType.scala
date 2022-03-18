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

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException
import org.apache.spark.sql.types.DataType


@JsonDeserialize(using=classOf[ArrayTypeDeserializer])
final case class ArrayType @JsonCreator(mode = JsonCreator.Mode.DISABLED) (
        @JsonProperty(value="elementType") elementType:FieldType,
        @JsonProperty(value="containsNull") containsNull:Boolean = true
    ) extends ContainerType {

    @JsonCreator(mode = JsonCreator.Mode.DEFAULT)
    def this() = { this(null, true) }

    /**
      * The Spark type to use
      *
      * @return
      */
    override def sparkType : org.apache.spark.sql.types.ArrayType = {
        org.apache.spark.sql.types.ArrayType(elementType.sparkType, containsNull)
    }

    /**
     * The Spark type to use
     * @return
     */
    override def catalogType : org.apache.spark.sql.types.ArrayType = {
        org.apache.spark.sql.types.ArrayType(elementType.catalogType, containsNull)
    }

    /**
      * Short Type Name as used in SQL and in YAML specification files
      * @return
      */
    override def sqlType : String = {
        "array<" + elementType.sqlType + ">"
    }

    override def parse(value:String, granularity:Option[String]=None) : Any = ???
    override def interpolate(value: FieldValue, granularity:Option[String]=None) : Iterable[Any] = ???
}

/*
 * This is a workaround for Jackson versions which do not use default values
 */
private class ArrayTypeDeserializer(vc:Class[_]) extends StdDeserializer[ArrayType](vc) {
    import java.io.IOException

    def this() = this(null)

    @throws[IOException]
    @throws[JsonProcessingException]
    def deserialize(jp: JsonParser, ctxt: DeserializationContext): ArrayType = {
        var currentToken: JsonToken = jp.nextValue()
        var elementType: FieldType = null
        var containsNull: Boolean = true
        while(currentToken != JsonToken.END_OBJECT) {
            jp.getCurrentName match {
                case "elementType" => elementType = jp.readValueAs(classOf[FieldType])
                case "containsNull" => containsNull = jp.getBooleanValue
                case _ => throw UnrecognizedPropertyException.from(jp, classOf[ArrayType], jp.getCurrentName, null)
            }
            currentToken = jp.nextValue()
        }
        ArrayType(elementType, containsNull)
    }
}
