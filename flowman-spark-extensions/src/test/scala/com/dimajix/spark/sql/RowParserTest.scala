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
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.util.DateTimeUtils


class RowParserTest extends AnyFlatSpec with Matchers {
    "The RowParser" should "work different data types" in {
        val lines = Seq(
            Array("1","lala","2.3","2019-02-01","2019-02-01T12:34:00.000"),
            Array("2","lolo","3.4","2019-02-02","2019-02-01T12:34:00"),
            Array("","","","",""),
            Array(null:String,null:String,null:String,null:String,null:String)
        )
        val schema = StructType(Seq(
            StructField("c1", IntegerType),
            StructField("c2", StringType),
            StructField("c3", DoubleType),
            StructField("c4", DateType),
            StructField("c5", TimestampType)
        ))
        val parser = new RowParser(schema, RowParser.Options())

        val result = lines.map(parser.parse)

        result should be (Seq(
            Row(1,"lala",2.3, Date.valueOf("2019-02-01"),new Timestamp(DateTimeUtils.stringToTime("2019-02-01T12:34:00").getTime)),
            Row(2,"lolo",3.4, Date.valueOf("2019-02-02"),new Timestamp(DateTimeUtils.stringToTime("2019-02-01T12:34:00").getTime)),
            Row(null,null,null,null,null),
            Row(null,null,null,null,null)
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
