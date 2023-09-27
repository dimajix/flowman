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

import java.sql.Date
import java.sql.Timestamp

import org.apache.spark.sql.Row
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

import com.dimajix.spark.testing.LocalSparkSession
import com.dimajix.util.DateTimeUtils


class DataFrameBuilderTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "DataFrameBuilder.ofStringValues" should "create a DataFrame" in {
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
        val df = DataFrameBuilder.ofStringValues(spark, lines, schema)

        df.collect() should be (Seq(
            Row(1,"lala",2.3, new java.math.BigDecimal("3.400000"), Date.valueOf("2019-02-01"),new Timestamp(DateTimeUtils.stringToTime("2019-02-01T12:34:00").getTime)),
            Row(2,"lolo",3.4, new java.math.BigDecimal("4.500000"), Date.valueOf("2019-02-02"),new Timestamp(DateTimeUtils.stringToTime("2019-02-01T12:34:00").getTime)),
            Row(null,"",null,null,null,null),
            Row(null,null,null,null,null,null)
        ))
        df.schema should be (schema)
    }

    "DataFrameBuilder.ofSchema" should "create an empty DataFrame" in {
        val schema = StructType(Seq(
            StructField("c1", IntegerType),
            StructField("c2", StringType),
            StructField("c3", DoubleType),
            StructField("c4", DateType)
        ))
        val df = DataFrameBuilder.ofSchema(spark, schema)

        df.collect() should be (Seq())
        df.schema should be (schema)
    }
}
