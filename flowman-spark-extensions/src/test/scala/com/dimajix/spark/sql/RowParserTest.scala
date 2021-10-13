/*
 * Copyright 2021 Kaya Kupferschmidt
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

import java.sql.Date
import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.BadRecordException
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.util.DateTimeUtils


class RowParserTest extends AnyFlatSpec with Matchers {
    def localTime(str:String) : Timestamp = new Timestamp(DateTimeUtils.stringToTime(str).getTime)
    def utcTime(str:String) : Timestamp = {
        val utcStr = if (str.contains('+')) str else str + "+00:00"
        new Timestamp(DateTimeUtils.stringToTime(utcStr).getTime)
    }

    "The RowParser" should "work different data types" in {
        val lines = Seq(
            Array("1","lala","2.3","3.4","2019-02-01","2019-02-01T12:34:00.000"),
            Array("2","lolo","3.4","4.5","2019-02-02","2019-02-01T12:34:00"),
            Array("","","","","",""),
            Array(null:String,null:String,null:String,null:String,null:String,null:String)
        )
        val schema = StructType(Seq(
            StructField("c1", IntegerType),
            StructField("c2", StringType),
            StructField("c3", DoubleType),
            StructField("c4", DecimalType(30,6)),
            StructField("c5", DateType),
            StructField("c6", TimestampType)
        ))
        val parser = new RowParser(schema, RowParser.Options())

        val result = lines.map(parser.parse)

        result should be (Seq(
            Row(1,"lala",2.3, new java.math.BigDecimal("3.400000"), Date.valueOf("2019-02-01"),localTime("2019-02-01T12:34:00")),
            Row(2,"lolo",3.4, new java.math.BigDecimal("4.500000"), Date.valueOf("2019-02-02"),localTime("2019-02-01T12:34:00")),
            Row(null,null,null,null,null,null),
            Row(null,null,null,null,null,null)
        ))
    }

    it should "support different timestamp formats" in {
        val lines = Seq(
            Array("2019-02-01T12:34:01.000000"),        // local time
            Array("2019-02-01T12:34:02.000"),           // local time
            Array("2019-02-01T12:34:03"),               // local time
            Array("2019-02-01T12:34:04.000+00:00"),     // utc time
            Array("2019-02-01T12:34:05+00:00"),         // utc time
            Array("2019-02-01 12:34:06")                // local time
        )
        val schema = StructType(Seq(
            StructField("c1", TimestampType)
        ))
        val parser = new RowParser(schema, RowParser.Options())

        val result = lines.map(parser.parse)

        result should be (Seq(
            Row(localTime("2019-02-01T12:34:01")),
            Row(localTime("2019-02-01T12:34:02")),
            Row(localTime("2019-02-01T12:34:03")),
            Row(utcTime("2019-02-01T12:34:04")),
            Row(utcTime("2019-02-01T12:34:05")),
            Row(localTime("2019-02-01T12:34:06"))
        ))

    }

    it should "accept fewer columns if told so" in {
        val lines = Seq(
            Array("1","lala"),
            Array("","")
        )
        val schema = StructType(Seq(
            StructField("c1", IntegerType),
            StructField("c2", StringType),
            StructField("c3", DoubleType)
        ))
        val parser = new RowParser(schema, RowParser.Options(addExtraColumns = true))

        val result = lines.map(parser.parse)

        result should be (Seq(
            Row(1,"lala",null),
            Row(null,null,null)
        ))
    }

    it should "throw exceptions on missing columns in strict mode" in {
        val lines = Seq(
            Array("1","lala")
        )
        val schema = StructType(Seq(
            StructField("c1", IntegerType),
            StructField("c2", StringType),
            StructField("c3", DoubleType)
        ))
        val parser = new RowParser(schema, RowParser.Options(addExtraColumns = false))

        a[BadRecordException] should be thrownBy (lines.map(parser.parse))
    }

    it should "accept extra columns if told so" in {
        val lines = Seq(
            Array("1","lala","23","y")
        )
        val schema = StructType(Seq(
            StructField("c1", IntegerType),
            StructField("c2", StringType),
            StructField("c3", DoubleType)
        ))
        val parser = new RowParser(schema, RowParser.Options(removeExtraColumns = true))

        val result = lines.map(parser.parse)

        result should be (Seq(
            Row(1,"lala",23.0)
        ))
    }

    it should "throw exceptions on extra columns in strict mode" in {
        val lines = Seq(
            Array("1","lala","x","y")
        )
        val schema = StructType(Seq(
            StructField("c1", IntegerType),
            StructField("c2", StringType),
            StructField("c3", DoubleType)
        ))
        val parser = new RowParser(schema, RowParser.Options(removeExtraColumns = false))

        a[BadRecordException] should be thrownBy (lines.map(parser.parse))
    }
}
