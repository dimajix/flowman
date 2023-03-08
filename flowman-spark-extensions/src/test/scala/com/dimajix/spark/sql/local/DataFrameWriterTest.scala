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

package com.dimajix.spark.sql.local

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.sql.local.implicits._
import com.dimajix.spark.testing.LocalSparkSession


class DataFrameWriterTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The DataFrameWriter" should "be instantiated by a readLocal call" in {
        val writer = spark.emptyDataFrame.writeLocal
        writer should not be (null)
        writer.mode("overwrite")
        writer.mode("append")
        writer.mode("ignore")
        writer.mode("error")
    }

}
