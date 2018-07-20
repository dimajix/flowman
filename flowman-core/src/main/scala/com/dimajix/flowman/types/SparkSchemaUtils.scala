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


object SparkSchemaUtils {
    def toSpark(schema:Seq[Field]) : org.apache.spark.sql.types.StructType = {
        org.apache.spark.sql.types.StructType(schema.map(_.sparkField))
    }

    def fromSpark(schema:org.apache.spark.sql.types.StructType) : Seq[Field] = {
        fromSpark(schema.fields)
    }
    def fromSpark(fields:Seq[org.apache.spark.sql.types.StructField]) : Seq[Field] = {
        fields.map(fromField)
    }

    private def fromField(field:org.apache.spark.sql.types.StructField) : Field = {
        val ftype = fromType(field.dataType)
        val description = field.getComment().orNull
        val size = if (field.metadata.contains("size")) Some(field.metadata.getLong("size").toInt) else None
        val default = if (field.metadata.contains("default")) field.metadata.getString("default") else null
        val format = if (field.metadata.contains("format")) field.metadata.getString("format") else null
        Field(field.name, ftype, field.nullable, description, default, size, format)
    }

    private def fromType(dataType:org.apache.spark.sql.types.DataType) : FieldType = {
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
            case org.apache.spark.sql.types.ArrayType(dt, n) => ArrayType(fromType(dt), n)
            case org.apache.spark.sql.types.StructType(fields) => StructType(fields.map(fromField))
            case org.apache.spark.sql.types.MapType(k,v,n) => MapType(fromType(k), fromType(v), n)
        }
    }
}
