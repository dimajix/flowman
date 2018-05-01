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

import java.util.Locale

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import org.apache.spark.sql.types.DataType

import com.dimajix.flowman.execution.Context


@JsonDeserialize(using=classOf[FieldTypeDeserializer])
abstract class FieldType {
    /**
      * The Spark type to use
      *
      * @return
      */
    def sparkType : DataType

    /**
      * Short Type Name as used in SQL and in YAML specification files
      * @return
      */
    def sqlType : String = {
        this.getClass.getSimpleName
        .stripSuffix("$").stripSuffix("Type")
        .toLowerCase(Locale.ROOT)
    }

    /**
      * Function for parsing a string as an instance of the given FieldType.
      * @param value
      * @return
      */
    def parse(value:String) : Any = parse(value, null)

    /**
      * Function for parsing a string as an instance of the given FieldType. This method
      * will also take into account any granularity.
      * @param value
      * @param granularity
      * @return
      */
    def parse(value:String, granularity:String) : Any

    /**
      * Function for interpolating a FieldValue as a sequence of the given FieldType. This method
      * will also take into account any granularity.
      * @param value
      * @param granularity
      * @return
      */
    def interpolate(value: FieldValue, granularity:String) : Iterable[Any]
}

@JsonDeserialize
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "array", value = classOf[ArrayType]),
    new JsonSubTypes.Type(name = "map", value = classOf[MapType]),
    new JsonSubTypes.Type(name = "struct", value = classOf[StructType])
))
abstract class ContainerType extends FieldType {

}


private object FieldTypeDeserializer {
    private val nonDecimalNameToType = {
        Seq(NullType, DateType, TimestampType, BinaryType, IntegerType, BooleanType, LongType,
            DoubleType, FloatType, ShortType, ByteType, StringType, CalendarIntervalType)
            .map(t => t.sqlType -> t).toMap ++
        Map("int" -> IntegerType, "text" -> StringType)
    }

    private val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
    private val VARCHAR = """varchar\(\s*(\d+)\s*\)""".r

    def deserialize(jp: JsonParser, ctxt: DeserializationContext): FieldType = {
        jp.getCurrentToken match {
            case JsonToken.VALUE_STRING => {
                jp.getText.toLowerCase match {
                    case "decimal" => DecimalType.USER_DEFAULT
                    case FIXED_DECIMAL (precision, scale) => DecimalType(precision.toInt, scale.toInt)
                    case VARCHAR(length) => VarcharType(length.toInt)
                    case other => nonDecimalNameToType.getOrElse (
                        other,
                        throw JsonMappingException.from(jp, s"Failed to convert the JSON string '${jp.getText}' to a field type.")
                    )
                }
            }
            case JsonToken.START_OBJECT => {
                jp.readValueAs(classOf[ContainerType])
            }
            case _ => throw JsonMappingException.from(jp, "Wrong type for FieldType")
        }
    }
}


private class FieldTypeDeserializer(vc:Class[_]) extends StdDeserializer[FieldType](vc) {
    import java.io.IOException

    def this() = this(null)

    @throws[IOException]
    @throws[JsonProcessingException]
    def deserialize(jp: JsonParser, ctxt: DeserializationContext): FieldType = {
        FieldTypeDeserializer.deserialize(jp, ctxt)
    }
}
