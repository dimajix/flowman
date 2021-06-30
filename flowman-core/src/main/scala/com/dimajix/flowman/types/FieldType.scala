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

import com.dimajix.flowman.types.DoubleType.parse


object FieldType {
    private val nonDecimalNameToType = {
        Seq(NullType, DateType, TimestampType, BinaryType, IntegerType, BooleanType, LongType,
            DoubleType, FloatType, ShortType, ByteType, StringType, CalendarIntervalType, DurationType)
            .map(t => t.sqlType -> t).toMap ++
            Map("byte" -> ByteType, "short" -> ShortType, "long" -> LongType, "int" -> IntegerType, "text" -> StringType)
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
            case org.apache.spark.sql.types.NullType => NullType
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
      * @return
      */
    def sparkType : DataType

    /**
      * The type to use in catalogs like Hive etc. In contrast to [[sparkType]], this method will keep VarChartype
      * and similar types, which are not used in Spark itself
      */
    def catalogType : DataType = sparkType

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


abstract class NumericType[T] extends FieldType {
    protected def parseRaw(value:String) : T
}
abstract class IntegralType[T : scala.math.Integral] extends NumericType[T] {
    import scala.math.Integral.Implicits._

    override def parse(value:String, granularity:Option[String]=None) : T =  {
        if (granularity.nonEmpty)
            roundDown(parseRaw(value), parseRaw(granularity.get))
        else
            parseRaw(value)
    }
    override def interpolate(value: FieldValue, granularity:Option[String]=None) : Iterable[T] = {
        value match {
            case SingleValue(v) => Seq(parse(v, granularity))
            case ArrayValue(values) => values.map(parse(_, granularity))
            case RangeValue(start,end,step) => {
                if (step.nonEmpty) {
                    val range = NumericRange(parseRaw(start), parseRaw(end), parseRaw(step.get))
                    if (granularity.nonEmpty) {
                        val mod = parseRaw(granularity.get)
                        range.map(x => roundDown(x, mod)).distinct
                    }
                    else {
                        range
                    }
                }
                else if (granularity.nonEmpty) {
                    val mod = parseRaw(granularity.get)
                    NumericRange(roundDown(parseRaw(start), mod), roundDown(parseRaw(end), mod), mod)
                }
                else {
                    val num = implicitly[Integral[T]]
                    NumericRange(parseRaw(start), parseRaw(end), num.one)
                }
            }
        }
    }

    private def roundDown(value:T, granularity:T) : T = {
        value / granularity * granularity
    }
    private def roundUp(value:T, granularity:T) : T = {
        val one = implicitly[Numeric[T]].one
        (value + granularity - one) / granularity * granularity
    }
}

abstract class FractionalType[T : Fractional] extends NumericType[T] {
    protected implicit def fractionalNum: Fractional[T]
    protected implicit def integralNum: Integral[T]

    override def parse(value:String, granularity:Option[String]=None) : T = {
        if (granularity.nonEmpty) {
            val step = parseRaw(granularity.get)
            val v = parseRaw(value)
            roundDown(v, step)
        }
        else {
            parseRaw(value)
        }
    }

    override def interpolate(value: FieldValue, granularity:Option[String]=None) : Iterable[T] = {
        value match {
            case SingleValue(v) => Seq(parse(v, granularity))
            case ArrayValue(values) => values.map(v => parse(v,granularity))
            case RangeValue(start,end,step) => {
                if (step.nonEmpty) {
                    val range = NumericRange(parseRaw(start), parseRaw(end), parseRaw(step.get))
                    if (granularity.nonEmpty) {
                        val mod = parseRaw(granularity.get)
                        range.map(x => roundDown(x, mod)).distinct
                    }
                    else {
                        range
                    }
                }
                else if (granularity.nonEmpty) {
                    val mod = parseRaw(granularity.get)
                    val range = NumericRange(parseRaw(start), parseRaw(end), mod)
                    range.map(x => roundDown(x, mod)).distinct
                }
                else {
                    NumericRange(parseRaw(start), parseRaw(end), fractionalNum.one)
                }
            }
        }
    }

    private def roundDown(value:T, granularity:T) : T = {
        val ops = integralNum

        ops.times(ops.quot(value, granularity), granularity)
    }
    private def roundUp(value:T, granularity:T) : T = {
        val one = fractionalNum.one
        val ops = integralNum
        ops.times(ops.quot(ops.minus(ops.plus(value, granularity), one), granularity), granularity)
    }
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


private class FieldTypeDeserializer(vc:Class[_]) extends StdDeserializer[FieldType](vc) {
    import java.io.IOException

    def this() = this(null)

    @throws[IOException]
    @throws[JsonProcessingException]
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
