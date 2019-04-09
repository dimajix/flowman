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

package com.dimajix.flowman.spec.flow

import org.apache.spark.sql.Row
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Module


class UpdateMappingTest extends FlatSpec with Matchers with LocalSparkSession {
    "The UpdateMapping" should "merge in updates" in {
        val mapping = UpdateMapping("prev", "updates", Seq("_1"), "_3 != 'DELETE'")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        implicit val context = executor.context

        val prev = executor.spark.createDataFrame(Seq(
            ("id-123", "will_remain"),
            ("id-124", "will_be_deleted"),
            ("id-125", "will_be_updated")
        ))
        val updates = executor.spark.createDataFrame(Seq(
            ("id-124", "will_be_deleted", "DELETE"),
            ("id-125", "will_be_updated", "UPDATE"),
            ("id-126", "will_be_added", "CREATE")
        ))

        val result = mapping.execute(executor, Map(MappingIdentifier("prev") -> prev, MappingIdentifier("updates") -> updates))
        val rows = result.orderBy("_1").collect().toSeq
        rows should be (Seq(
            Row("id-123", "will_remain"),
            Row("id-125", "will_be_updated"),
            Row("id-126", "will_be_added")
        ))
    }

    it should "add missing columns from updates" in {
        val mapping = UpdateMapping("prev", "updates", Seq("_1"), "op != 'DELETE'")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        implicit val context = executor.context

        val prev = executor.spark.createDataFrame(Seq(
            ("id-123", "will_remain", "col3"),
            ("id-124", "will_be_deleted", "col3"),
            ("id-125", "will_be_updated", "col3")
        ))
        val updates = executor.spark.createDataFrame(Seq(
            ("id-124", "will_be_deleted", "DELETE"),
            ("id-125", "will_be_updated", "UPDATE"),
            ("id-126", "will_be_added", "CREATE")
        )).withColumnRenamed("_3", "op")

        val result = mapping.execute(executor, Map(MappingIdentifier("prev") -> prev, MappingIdentifier("updates") -> updates))
        val rows = result.orderBy("_1").collect().toSeq
        rows should be (Seq(
            Row("id-123", "will_remain", "col3"),
            Row("id-125", "will_be_updated", null),
            Row("id-126", "will_be_added", null)
        ))
    }

    "An appropriate Dataflow" should "be readable from YML" in {
        val spec =
            """
              |mappings:
              |  t1:
              |    kind: update
              |    input: t0
              |    updates: t1
              |    filter: "operation != 'DELETE'"
              |    keyColumns: id
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        implicit val context = executor.context

        project.mappings.size should be (1)
        project.mappings.contains("t0") should be (false)
        project.mappings.contains("t1") should be (true)

        val mapping = project.mappings("t1")
        mapping shouldBe an[UpdateMapping]
        val updateMapping = mapping.asInstanceOf[UpdateMapping]
        updateMapping.dependencies.toSeq should be (Seq(MappingIdentifier("t0"),MappingIdentifier("t1")))
        updateMapping.input should be (MappingIdentifier("t0"))
        updateMapping.updates should be (MappingIdentifier("t1"))
        updateMapping.keyColumns should be (Seq("id"))
        updateMapping.filter should be ("operation != 'DELETE'")
    }
}
