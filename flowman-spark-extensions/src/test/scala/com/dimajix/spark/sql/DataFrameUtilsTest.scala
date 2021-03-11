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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.testing.LocalSparkSession


class DataFrameUtilsTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "DataFrameUtils.ofCsvRows" should "create a DataFrame" in {
        val lines = Seq(
            "1,lala,2.3",
            "2,lolo,3.4",
            ",,",
            "3,\"\",4.5"
        )
        val schema = StructType(Seq(
            StructField("c1", IntegerType),
            StructField("c2", StringType),
            StructField("c3", DoubleType)
        ))
        val df = DataFrameUtils.ofCsvRows(spark, lines, schema)

        df.collect() should be (Seq(
            Row(1,"lala",2.3),
            Row(2,"lolo",3.4),
            Row(null,null,null),
            Row(3,null,4.5)
        ))
        df.schema should be (schema)
    }

    "DataFrameUtils.ofStringValues" should "create a DataFrame" in {
        val lines = Seq(
            Array("1","lala","2.3"),
            Array("2","","3.4"),
            Array("",null,"")
        )
        val schema = StructType(Seq(
            StructField("c1", IntegerType),
            StructField("c2", StringType),
            StructField("c3", DoubleType)
        ))
        val df = DataFrameUtils.ofStringValues(spark, lines, schema)

        df.collect() should be (Seq(
            Row(1,"lala",2.3),
            Row(2,null,3.4),
            Row(null,null,null)
        ))
        df.schema should be (schema)
    }

    "DataFrameUtils.ofSchema" should "create an empty DataFrame" in {
        val schema = StructType(Seq(
            StructField("c1", IntegerType),
            StructField("c2", StringType),
            StructField("c3", DoubleType)
        ))
        val df = DataFrameUtils.ofSchema(spark, schema)

        df.collect() should be (Seq())
        df.schema should be (schema)
    }

    "DataFrameUtils.compare" should "work" in {
        val schema = StructType(Seq(
            StructField("c1", IntegerType),
            StructField("c2", StringType),
            StructField("c3", DoubleType)
        ))
        val lines1 = Seq(
            Array("1","lala","2.3"),
            Array("2","","3.4"),
            Array("",null,"")
        )
        val lines2 = Seq(
            Array("1","lala","2.3"),
            Array("2","","3.4"),
            Array("",null,"")
        )
        val df1 = DataFrameUtils.ofStringValues(spark, lines1, schema)
        val df2 = DataFrameUtils.ofStringValues(spark, lines2, schema)

        DataFrameUtils.compare(df1, df2) should be (true)
        DataFrameUtils.compare(df1.limit(2), df2) should be (false)
        DataFrameUtils.compare(df1, df2.limit(2)) should be (false)
        DataFrameUtils.compare(df1.drop("c1"), df2) should be (false)
        DataFrameUtils.compare(df1, df2.drop("c1")) should be (false)
    }

    "DataFrameUtils.diff" should "work" in {
        val schema = StructType(Seq(
            StructField("c1", IntegerType),
            StructField("c2", StringType),
            StructField("c3", DoubleType)
        ))
        val lines1 = Seq(
            Array("1","lala","2.3"),
            Array("2","","3.4"),
            Array("",null,"")
        )
        val lines2 = Seq(
            Array("1","lala","2.3"),
            Array("2","","3.4"),
            Array("",null,"")
        )
        val df1 = DataFrameUtils.ofStringValues(spark, lines1, schema)
        val df2 = DataFrameUtils.ofStringValues(spark, lines2, schema)

        DataFrameUtils.diff(df1, df2) should be (None)
        DataFrameUtils.diff(df1.limit(2), df2) should not be (None)
        DataFrameUtils.diff(df1, df2.limit(2)) should not be (None)
    }

    "DataFrameUtils.diffToStringValues" should "work" in {
        val schema = StructType(Seq(
            StructField("c1", IntegerType),
            StructField("c2", StringType),
            StructField("c3", DoubleType)
        ))
        val lines1 = Seq(
            Array("1","lala","2.3"),
            Array("2","","3.4"),
            Array("",null,"")
        )
        val lines2 = Seq(
            Array("1","lala","2.3"),
            Array("2","","3.4"),
            Array("",null,"")
        )
        val df1 = DataFrameUtils.ofStringValues(spark, lines1, schema)

        DataFrameUtils.diffToStringValues(df1, lines2) should be (None)
        DataFrameUtils.diffToStringValues(df1.limit(2), lines2) should not be (None)
        DataFrameUtils.diffToStringValues(df1, lines2.take(2)) should not be (None)
    }
}
