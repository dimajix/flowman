/*
 * Copyright 2019 Kaya Kupferschmidt
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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.sql.execution.ExtraStrategies
import com.dimajix.spark.sql.functions._
import com.dimajix.spark.testing.LocalSparkSession

class FunctionsTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "count_records" should "work" in {
        ExtraStrategies.register(spark)
        val df = spark.createDataFrame(Seq((1,2), (3,4)))
        val counter = spark.sparkContext.longAccumulator
        val result = count_records(df, counter)
        result.count() should be (2)
        counter.value should be (2)
    }
}
