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

package com.dimajix.flowman.types

import java.util.Locale

import scala.collection.immutable.NumericRange

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


object FieldType {
    private val nonDecimalNameToType = {
        Seq(NullType, DateType, TimestampType, BinaryType, IntegerType, BooleanType, LongType,
            DoubleType, FloatType, ShortType, ByteType, StringType, CalendarIntervalType, DurationType)
            .map(t => t.sqlType -> t).toMap ++
            Map("int" -> IntegerType, "text" -> StringType)
    }

    private val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
    private val VARCHAR = """varchar\(\s*(\d+)\s*\)""".r
    private val CHAR = """char\(\s*(\d+)\s*\)""".r

    /**
      * Create a Flowman FieldType from a Spark DataType
      * @param dataType
      * @return
      */
    def of(dataType:org.apache.spark.sql.types.DataType) : FieldType = {
        dataType match {
            case org.apache.spark.sql.types.ShortType => ShortType
            case org.apache.spark.sql.types.IntegerType => IntegerType
            case org.apache.spark.sql.types.LongType => LongType
            case org.apache.spark.sql.types.FloatType => FloatType
            case org.apache.spark.sql.types.DoubleType => DoubleType
            case d:org.apache.spark.sql.types.DecimalType => DecimalType(d.precision, d.scale)
            case org.apache.spark.sql.types.StringType => StringType
            case org.apache.spark.sql.types.ByteType => ByteType
            case org.apache.spark.sql.types.BinaryType => BinaryType
            case org.apache.spark.sql.types.BooleanType => BooleanType
            case org.apache.spark.sql.types.CharType(n) => CharType(n)
            case org.apache.spark.sql.types.VarcharType(n) => VarcharType(n)
            case org.apache.spark.sql.types.TimestampType => TimestampType
            case org.apache.spark.sql.types.DateType => DateType
            case org.apache.spark.sql.types.CalendarIntervalType => CalendarIntervalType
            case org.apache.spark.sql.types.ArrayType(dt, n) => ArrayType(FieldType.of(dt), n)
            case org.apache.spark.sql.types.StructType(fields) => StructType(fields.map(Field.of))
            case org.apache.spark.sql.types.MapType(k,v,n) => MapType(FieldType.of(k), FieldType.of(v), n)
        }
    }

    /**
      * Create a Flowman FieldType from a string
      * @param str
      * @return
      */
    def of(str:String) : FieldType = {
        str.toLowerCase match {
            case "decimal" => DecimalType.USER_DEFAULT
            case FIXED_DECIMAL (precision, scale) => DecimalType(precision.toInt, scale.toInt)
            case VARCHAR(length) => VarcharType(length.toInt)
            case CHAR(length) => CharType(length.toInt)
            case other => nonDecimalNameToType.getOrElse (
                other,
                throw new IllegalArgumentException(s"Failed to convert string '$str' to a field type.")
            )
        }
    }
}


@JsonDeserialize(using=classOf[FieldTypeDeserializer])
abstract class FieldType {
    /**
      * The Spark type to use
      *
      * @return
      */
    def sparkType : DataType

    /** Name of the type used in JSON serialization. */
    def typeName: String = {
        this.getClass.getSimpleName
            .stripSuffix("$").stripSuffix("Type")
            .toLowerCase(Locale.ROOT)
    }

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
      * Function for parsing a string as an instance of the given FieldType. This method
      * will also take into account any granularity.
      * @param value
      * @param granularity
      * @return
      */
    def parse(value:String, granularity:Option[String]=None) : Any

    /**
      * Function for interpolating a FieldValue as a sequence of the given FieldType. This method
      * will also take into account any granularity.
      * @param value
      * @param granularity
      * @return
      */
    def interpolate(value: FieldValue, granularity:Option[String]=None) : Iterable[Any]
}


abstract class NumericType[T : scala.math.Numeric] extends FieldType {
}
abstract class IntegralType[T : scala.math.Integral] extends NumericType[T] {
    import scala.math.Integral.Implicits._

    protected def parseRaw(value:String) : T

    override def parse(value:String, granularity:Option[String]=None) : T =  {
        if (granularity.nonEmpty)
            (parseRaw(value) / parseRaw(granularity.get) * parseRaw(granularity.get))
        else
            parseRaw(value)
    }
    override def interpolate(value: FieldValue, granularity:Option[String]=None) : Iterable[T] = {
        def one(implicit ops:Integral[T]) : T = ops.one

        value match {
            case SingleValue(v) => Seq(parse(v, granularity))
            case ArrayValue(values) => values.map(parse(_, granularity))
            case RangeValue(start,end,step) => {
                if (step.nonEmpty) {
                    val range = NumericRange(parseRaw(start), parseRaw(end), parseRaw(step.get))
                    if (granularity.nonEmpty) {
                        val mod = parseRaw(granularity.get)
                        range.map(_ / mod * mod).distinct
                    }
                    else {
                        range
                    }
                }
                else if (granularity.nonEmpty) {
                    val mod = parseRaw(granularity.get)
                    NumericRange(parseRaw(start) / mod * mod, parseRaw(end) / mod * mod, mod)
                }
                else {
                    NumericRange(parseRaw(start), parseRaw(end), one)
                }
            }
        }
    }
}
abstract class FractionalType[T : scala.math.Fractional] extends NumericType[T] {
    protected def parseRaw(value:String) : T
}


@JsonDeserialize
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "array", value = classOf[ArrayType]),
    new JsonSubTypes.Type(name = "map", value = classOf[MapType]),
    new JsonSubTypes.Type(name = "struct", value = classOf[StructType])
))
abstract class ContainerType extends FieldType {

}


private object FieldTypeDeserializer {
    def deserialize(jp: JsonParser, ctxt: DeserializationContext): FieldType = {
        jp.getCurrentToken match {
            case JsonToken.VALUE_STRING => {
                try {
                    FieldType.of(jp.getText)
                }
                catch {
                    case _:IllegalArgumentException =>
                        throw JsonMappingException.from(jp, s"Failed to convert the JSON string '${jp.getText}' to a field type.")
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
