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

package com.dimajix.flowman.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.testing.LocalSparkSession


class ConsoleUtilsTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    val inputJson =
        """
          |{
          |  "int_field":123,
          |  "bool_field":true,
          |  "null_field":null
          |}""".stripMargin

    "ConsoleUtils" should "show DataFrames as CSV" in {
        val spark = this.spark
        import spark.implicits._

        val inputRecords = Seq(inputJson.replace("\n",""))
        val inputDs = spark.createDataset(inputRecords)
        val df = spark.read.json(inputDs)

        ConsoleUtils.showDataFrame(df, csv=true)
    }

    it should "show DataFrames" in {
        val spark = this.spark
        import spark.implicits._

        val inputRecords = Seq(inputJson.replace("\n",""))
        val inputDs = spark.createDataset(inputRecords)
        val df = spark.read.json(inputDs)

        ConsoleUtils.showDataFrame(df, csv=false)
    }
}
