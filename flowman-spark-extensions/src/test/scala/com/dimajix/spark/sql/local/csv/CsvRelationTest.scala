/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.spark.sql.local.csv

import java.io.File
import java.sql.Date
import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.sql.local.implicits._
import com.dimajix.spark.testing.LocalSparkSession


class CsvRelationTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    private def ts(str:String) : Timestamp = new Timestamp(Instant.parse(str).toEpochMilli)


    "The local CSV relation" should "support writing CSV files" in {
        val df = spark.createDataFrame(Seq(
            (1,"lala", 1.2, ts("2020-01-02T23:12:31.0Z"), Date.valueOf("2020-01-02")),
            (2,"lolo", 2.3, ts("2021-03-02T21:12:31.0Z"), Date.valueOf("2020-02-02"))
        ))
        df.writeLocal
            .format("csv")
            .option("encoding", "UTF-8")
            .option("header", true)
            .save(new File(tempDir, "lala.csv"), SaveMode.Overwrite)
    }

    it should "support reading CSV files" in {
        val schema = StructType(
            StructField("int_field", IntegerType) ::
                StructField("str_field", StringType) ::
                StructField("double_field", DoubleType) ::
                StructField("timestamp_field", TimestampType) ::
                StructField("date_field", DateType) ::
                Nil
        )
        val result = spark.readLocal
            .format("csv")
            .schema(schema)
            .option("encoding", "UTF-8")
            .option("header", true)
            .load(new File(tempDir, "lala.csv"))

        result.schema should be (schema)
        result.count() should be (2)
        result.collect() should be (Seq(
            Row(1,"lala", 1.2, ts("2020-01-02T23:12:31.0Z"), Date.valueOf("2020-01-02")),
            Row(2,"lolo", 2.3, ts("2021-03-02T21:12:31.0Z"), Date.valueOf("2020-02-02"))
        ))

        result.writeLocal
            .format("csv")
            .option("encoding", "UTF-8")
            .option("header", true)
            .save(new File(tempDir, "lala2.csv"), SaveMode.Overwrite)
    }

    it should "support schema inference with header" in {
        val schema = StructType(
            StructField("int_field", StringType) ::
                StructField("str_field", StringType) ::
                StructField("double_field", StringType) ::
                StructField("timestamp_field", StringType) ::
                StructField("date_field", StringType) ::
                Nil
        )
        val result = spark.readLocal
            .format("csv")
            .option("encoding", "UTF-8")
            .option("header", true)
            .load(new File(tempDir, "lala2.csv"))

        result.schema should be (schema)
        result.count() should be (2)
        result.collect() should be (Seq(
            Row("1","lala", "1.2", "2020-01-02T23:12:31.000Z", "2020-01-02"),
            Row("2","lolo", "2.3", "2021-03-02T21:12:31.000Z", "2020-02-02")
        ))
    }

    it should "support schema inference without header" in {
        val schema = StructType(
            StructField("_1", StringType) ::
                StructField("_2", StringType) ::
                StructField("_3", StringType) ::
                StructField("_4", StringType) ::
                StructField("_5", StringType) ::
                Nil
        )
        val result = spark.readLocal
            .format("csv")
            .option("encoding", "UTF-8")
            .option("header", false)
            .load(new File(tempDir, "lala2.csv"))

        result.schema should be (schema)
        result.count() should be (3)
        result.collect() should be (Seq(
            Row("int_field", "str_field", "double_field", "timestamp_field", "date_field"),
            Row("1","lala", "1.2", "2020-01-02T23:12:31.000Z", "2020-01-02"),
            Row("2","lolo", "2.3", "2021-03-02T21:12:31.000Z", "2020-02-02")
        ))
    }
}
