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

package com.dimajix.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.sql.functions._
import com.dimajix.spark.testing.LocalSparkSession


object NullableStructTest {
    case class Person(name:String, age:Option[Int])
}

class NullableStructTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    import NullableStructTest.Person

    "The nullable_struct function" should "return non-null values" in {
        val df = spark.createDataFrame(Seq(
            Person("Bob", Some(23)),
            Person("Alice", None),
            Person(null, None)
        ))

        val result = df.select(nullable_struct(df("name"), df("age")) alias "s")
        val rows = result.orderBy(col("s.name"))
            .collect()
        rows.toSeq should be (Seq(
            Row(null),
            Row(Row("Alice", null)),
            Row(Row("Bob", 23))
        ))
    }

    it should "work with unresolved columns" in {
        val df = spark.createDataFrame(Seq(
            Person("Bob", Some(23)),
            Person("Alice", None),
            Person(null, None)
        ))

        val result = df.repartition(2).select(nullable_struct(col("name"), col("age")) alias "s")
        //result.queryExecution.debug.codegen()
        val rows = result.orderBy(col("s.name"))
            .collect()
        rows.toSeq should be (Seq(
            Row(null),
            Row(Row("Alice", null)),
            Row(Row("Bob", 23))
        ))
    }
}
