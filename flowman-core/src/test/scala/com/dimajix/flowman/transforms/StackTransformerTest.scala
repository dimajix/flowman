/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.transforms

import java.sql.Date

import scala.collection.immutable.ListMap

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.{types => ftypes}
import com.dimajix.spark.testing.LocalSparkSession


class StackTransformerTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The StackTransformer" should "work" in {
        val df = spark.createDataFrame(Seq(
            (1,"lala", 33, Some(1.2), Date.valueOf("2020-01-02")),
            (2,"lolo", 44, None, Date.valueOf("2020-02-02"))
        ))

        val xfs = StackTransformer(
            "col",
            "value",
            ListMap("_3" -> "col3", "_4" -> "col4")
        )

        val expectedSchema = StructType(Seq(
            StructField("_1", IntegerType, false),
            StructField("_2", StringType, true),
            StructField("_5", DateType, true),
            StructField("col", StringType, false),
            StructField("value", DoubleType, true)
        ))

        val resultDf = xfs.transform(df)
        resultDf.schema should be (expectedSchema)
        resultDf.count should be (3)
        resultDf.collect() should be (Seq(
            Row(1,"lala", Date.valueOf("2020-01-02"), "col3", 33.0),
            Row(2,"lolo", Date.valueOf("2020-02-02"), "col3", 44.0),
            Row(1,"lala", Date.valueOf("2020-01-02"), "col4", 1.2)
        ))

        val resultSchema = xfs.transform(ftypes.StructType.of(df.schema))
        resultSchema should be (ftypes.StructType.of(expectedSchema))
    }
}
