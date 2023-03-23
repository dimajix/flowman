/*
 * Copyright (C) 2018 The Flowman Authors
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

package com.dimajix.spark.sql.sources.fixedwidth

import scala.util.control.NonFatal

import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType


object FixedWidthUtils {
    /**
      * Verify if the schema is supported in fixed widtth datasource.
      */
    def verifySchema(schema: StructType): Unit = {
        def verifyType(dataType: DataType): Unit = dataType match {
            case ByteType | ShortType | IntegerType | LongType | FloatType |
                 DoubleType | BooleanType | _: DecimalType | TimestampType |
                 DateType | StringType =>

            case _ =>
                throw new UnsupportedOperationException(
                    s"Fixed Width data source does not support ${dataType.simpleString} data type.")
        }

        def verifySize(field:StructField) : Unit = {
            if (!field.metadata.contains("size"))
                throw new IllegalArgumentException(
                    s"Fixed Width data source requires 'size' metadata in field ${field.name}.")
            try {
                field.metadata.getLong("size")
            }
            catch {
                case NonFatal(ex) => throw new IllegalArgumentException(
                    s"Invalid metadata 'size' metadata in field ${field.name}.", ex)
            }
        }

        schema.foreach(field => verifyType(field.dataType))
        schema.foreach(field => verifySize(field))
    }

}
