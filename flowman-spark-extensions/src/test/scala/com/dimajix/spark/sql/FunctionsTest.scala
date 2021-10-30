/*
 * Copyright 2019-2021 Kaya Kupferschmidt
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

import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.sql.DataFrameUtils.withTempView
import com.dimajix.spark.sql.FunctionsTest.columnFunction
import com.dimajix.spark.sql.execution.ExtraStrategies
import com.dimajix.spark.sql.functions._
import com.dimajix.spark.testing.LocalSparkSession
import com.dimajix.spark.testing.QueryTest


object FunctionsTest {
    def columnFunction(l:Column, r:Column) : Column = concat(l,r)
}


class FunctionsTest extends AnyFlatSpec with Matchers with LocalSparkSession with QueryTest {
    override def beforeAll() : Unit = {
        super.beforeAll()
        register(spark, columnFunction _, "cfn")
    }
    override def configureSpark(builder: SparkSession.Builder) : SparkSession.Builder = {
        builder.withExtensions { extensions =>
            // Doesn't work with Spark 2.x
            // extensions.injectFunction(wrap(columnFunction _, "cfn"))
        }
    }

    "count_records" should "work" in {
        ExtraStrategies.register(spark)
        val df = spark.createDataFrame(Seq((1,2), (3,4)))
        val counter = spark.sparkContext.longAccumulator
        val result = count_records(df, counter)
        result.count() should be (2)
        counter.value should be (2)
    }

    "nullable_struct" should "work" in {
        val df = spark.createDataFrame(Seq((Some(1),Some(2)), (None,Some(4)), (Some(5), None), (None, None)))
        val result = df.select(nullable_struct(col("_1"), col("_2")).as("x"))

        result.schema should be (StructType(Seq(
            StructField("x", StructType(Seq(
                StructField("_1", IntegerType),
                StructField("_2", IntegerType)
            )))
        )))

        checkAnswer(result, Seq(
            Row(Row(1,2)),
            Row(Row(null,4)),
            Row(Row(5,null)),
            Row(null)
        ))
    }

    "wrapping functions" should "work" in {
        val df = spark.createDataFrame(Seq((1,2), (3,4)))
        val result = withTempView("df", df) {
            spark.sql("SELECT cfn(_1,_2) AS cfn FROM df")
        }

        result.schema should be (StructType(Seq(
            StructField("cfn", StringType, false)
        )))

        checkAnswer(result, Seq(
            Row("12"),
            Row("34")
        ))
    }
}
