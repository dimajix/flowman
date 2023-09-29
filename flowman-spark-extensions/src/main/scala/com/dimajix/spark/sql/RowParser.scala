/*
 * Copyright (C) 2021 The Flowman Authors
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

package com.dimajix.spark.sql

import java.math.BigDecimal
import java.sql.Timestamp
import java.text.NumberFormat
import java.util.Locale
import java.util.TimeZone

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkShim.newBadRecordException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.util.BadRecordException
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.unsafe.types.UTF8String

import com.dimajix.util.DateTimeUtils


object RowParser {
    object Options {
        def apply(addExtraColumns:Boolean = false,
                  removeExtraColumns:Boolean = false
                 ) : Options = {
            val timeZone = DateTimeUtils.getTimeZone("UTC")
            Options(
                nullValue = null,
                nanValue = "NaN",
                negativeInf = "Inf",
                positiveInf = "-Inf",
                timeZone = timeZone,
                timestampFormat = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", timeZone, Locale.US),
                dateFormat = FastDateFormat.getInstance("yyyy-MM-dd", Locale.US),
                addExtraColumns = addExtraColumns,
                removeExtraColumns = removeExtraColumns
            )
        }
    }
    case class Options(
        nullValue:String,
        nanValue:String,
        negativeInf:String,
        positiveInf:String,
        timeZone:TimeZone,
        timestampFormat:FastDateFormat,
        dateFormat:FastDateFormat,
        addExtraColumns:Boolean,
        removeExtraColumns:Boolean
    )
}


class RowParser(schema: StructType, options:RowParser.Options) {
    // A `ValueConverter` is responsible for converting the given value to a desired type.
    private type ValueConverter = String => Any
    private val valueConverters: Array[ValueConverter] =
        schema.map(f => makeConverter(f.name, f.dataType, f.nullable)).toArray
    private val tokenIndexArr: Array[Int] =
        schema.map(f => schema.indexOf(f)).toArray

    /**
     * Create a converter which converts the string value to a value according to a desired type.
     * Currently, we do not support complex types (`ArrayType`, `MapType`, `StructType`).
     *
     * For other nullable types, returns null if it is null or equals to the value specified
     * in `nullValue` option.
     */
    private def makeConverter(
        name: String,
        dataType: DataType,
        nullable: Boolean = true): ValueConverter = dataType match {
        case _: ByteType => (d: String) =>
            nullSafeDatum(d, name, nullable)(_.toByte)

        case _: ShortType => (d: String) =>
            nullSafeDatum(d, name, nullable)(_.toShort)

        case _: IntegerType => (d: String) =>
            nullSafeDatum(d, name, nullable)(_.toInt)

        case _: LongType => (d: String) =>
            nullSafeDatum(d, name, nullable)(_.toLong)

        case _: FloatType => (d: String) =>
            nullSafeDatum(d, name, nullable) {
                case options.nanValue => Float.NaN
                case options.negativeInf => Float.NegativeInfinity
                case options.positiveInf => Float.PositiveInfinity
                case datum =>
                    Try(datum.toFloat)
                        .getOrElse(NumberFormat.getInstance(Locale.US).parse(datum).floatValue())
            }

        case _: DoubleType => (d: String) =>
            nullSafeDatum(d, name, nullable) {
                case options.nanValue => Double.NaN
                case options.negativeInf => Double.NegativeInfinity
                case options.positiveInf => Double.PositiveInfinity
                case datum =>
                    Try(datum.toDouble)
                        .getOrElse(NumberFormat.getInstance(Locale.US).parse(datum).doubleValue())
            }

        case _: BooleanType => (d: String) =>
            nullSafeDatum(d, name, nullable)(_.toBoolean)

        case dt: DecimalType => (d: String) =>
            nullSafeDatum(d, name, nullable) { datum =>
                val value = new BigDecimal(datum.replaceAll(",", ""))
                //Decimal(value, dt.precision, dt.scale)
                value
            }

        case _: TimestampType => (d: String) =>
            nullSafeDatum(d, name, nullable) { datum =>
                // This one will lose microseconds parts.
                // See https://issues.apache.org/jira/browse/SPARK-10681.
                Try {
                    new Timestamp(options.timestampFormat.parse(datum).getTime)
                }.getOrElse {
                    // If it fails to parse, then tries the way used in 2.0 and 1.x for backwards
                    // compatibility.
                    new Timestamp(DateTimeUtils.stringToTime(datum).getTime)
                }
            }

        case _: DateType => (d: String) =>
            nullSafeDatum(d, name, nullable) { datum =>
                java.sql.Date.valueOf(datum)
            }

        case _: StringType => (datum: String) =>
            if (datum == options.nullValue || datum == null) {
                if (!nullable) {
                    throw new RuntimeException(s"null value found but field $name is not nullable.")
                }
                null
            } else {
                datum
            }

        // We don't actually hit this exception though, we keep it for understandability
        case _ => throw new RuntimeException(s"Unsupported type: ${dataType.typeName}")
    }

    private def nullSafeDatum(
        datum: String,
        name: String,
        nullable: Boolean)(converter: ValueConverter): Any = {
        if (datum == options.nullValue || datum == null || datum.isEmpty) {
            if (!nullable) {
                throw new RuntimeException(s"null value found but field $name is not nullable.")
            }
            null
        } else {
            converter.apply(datum)
        }
    }

    def parse(tokens: Array[String]): Row = {
        parseInternal(tokens, None)
    }
    def parse(tokens: Array[String], line:String): Row = {
        parseInternal(tokens, Some(line))
    }

    private def parseInternal(tokens: Array[String], line:Option[String]): Row = {
        // If the number of tokens doesn't match the schema, we should treat it as a malformed record.
        // However, we still have chance to parse some of the tokens, by adding extra null tokens in
        // the tail if the number is smaller, or by dropping extra tokens if the number is larger.
        if ((tokens.length < schema.length && !options.addExtraColumns) ||
            (tokens.length > schema.length && !options.removeExtraColumns)
        ) {
            throw newBadRecordException(
                new RuntimeException(s"Malformed record. Expected ${schema.length} columns, but only got ${tokens.length} columns. Offending record:\n${line.getOrElse(tokens.mkString(","))}")
            )
        }

        val checkedTokens = if (schema.length > tokens.length) {
            tokens ++ new Array[String](schema.length - tokens.length)
        } else {
            tokens.take(schema.length)
        }

        try {
            val values = (0 until schema.length).map { i =>
                val from = tokenIndexArr(i)
                valueConverters(from).apply(checkedTokens(from))
            }
            new GenericRowWithSchema(values.toArray, schema)
        } catch {
            case NonFatal(e) =>
                throw newBadRecordException(e)
        }
    }
}

