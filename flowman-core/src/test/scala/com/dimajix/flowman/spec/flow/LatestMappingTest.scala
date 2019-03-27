/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.flow

import scala.collection.mutable

import org.apache.spark.sql.Row
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.ObjectMapper


class LatestMappingTest extends FlatSpec with Matchers with LocalSparkSession {
    "The LatestMapping" should "extract the latest version" in {
        val spark = this.spark
        import spark.implicits._

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        implicit val context = executor.context

        val json_1 = Seq(
            """{"ts":123,"id":12, "a":[12,2], "op":"CREATE"}""",
            """{"ts":125,"id":12, "a":[12,2], "op":"UPDATE"}""",
            """{"ts":133,"id":12, "a":[12,3], "op":"UPDATE"}""",
            """{"ts":134,"id":12, "a":[12,4], "op":"DELETE"}""",
            """{"ts":123,"id":13, "a":[13,2], "op":"CREATE"}""",
            """{"ts":123,"id":14, "a":[14,2], "op":"CREATE"}""",
            """{"ts":124,"id":14, "a":[14,3], "op":"UPDATE"}""",
            """{"ts":127,"id":15, "a":[15,2], "op":"CREATE"}"""
        ).toDS
        val df = spark.read.json(json_1)

        val mapping = LatestMapping("df1", Seq("id"), "ts")
        mapping.input should be (MappingIdentifier("df1"))
        mapping.keyColumns should be (Seq("id" ))
        mapping.versionColumn should be ("ts")
        mapping.dependencies should be (Array(MappingIdentifier("df1")))

        val result = mapping.execute(executor, Map(MappingIdentifier("df1") -> df)).orderBy("id").collect()
        result.size should be (4)
        result(0) should be (Row(mutable.WrappedArray.make(Array(12,4)), 12, "DELETE", 134))
        result(1) should be (Row(mutable.WrappedArray.make(Array(13,2)), 13, "CREATE", 123))
        result(2) should be (Row(mutable.WrappedArray.make(Array(14,3)), 14, "UPDATE", 124))
        result(3) should be (Row(mutable.WrappedArray.make(Array(15,2)), 15, "CREATE", 127))
    }

    it should "be parseable" in {
        val spec =
            """
              |kind: latest
              |input: df1
              |keyColumns:
              | - id
              |versionColumn: v
            """.stripMargin
        val mapping = ObjectMapper.parse[Mapping](spec)
        val session = Session.builder().build()
        val executor = session.executor
        implicit val context = executor.context

        mapping shouldBe a[LatestMapping]
        val latest = mapping.asInstanceOf[LatestMapping]
        latest.input should be (MappingIdentifier("df1"))
        latest.keyColumns should be (Seq("id"))
        latest.versionColumn should be ("v")
    }
}
