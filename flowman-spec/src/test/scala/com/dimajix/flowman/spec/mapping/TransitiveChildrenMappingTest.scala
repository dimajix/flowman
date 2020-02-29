/*
 * Copyright 2018-2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.mapping

import org.apache.spark.sql.types._
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.{types => ftypes}
import com.dimajix.spark.testing.LocalSparkSession


class TransitiveChildrenMappingTest extends FlatSpec with Matchers with LocalSparkSession {
    "A TransitiveMapping" should "be parseable" in {
        val spec =
          """
            |mappings:
            |  children_latest:
            |    kind: transitiveChildren
            |    input: organization_child_relation
            |    parentColumns: parent_account_id
            |    childColumns: child_account_id
          """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val mapping = project.mappings("children_latest")

        mapping shouldBe an[TransitiveChildrenMappingSpec]
    }

    it should "generate the expected schema and results" in {
        val spark = this.spark
        import spark.implicits._

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val inputDf = Seq(
            (Some(1),Some(2)),
            (Some(1),Some(3)),
            (Some(2),Some(4)),
            (Some(4),Some(5)),
            (Some(4),Some(6)),
            (None,Some(7)),
            (Some(8),None)
        ).toDF("parent_id", "child_id")

        val mapping = new TransitiveChildrenMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input_df"),
            Seq("parent_id"),
            Seq("child_id"),
            true
        )

        val expectedSchema = StructType(Seq(
            StructField("parent_id", IntegerType, true),
            StructField("child_id", IntegerType, true)
        ))
        val expectedResult = Seq(
            (1,1),
            (1,2),
            (1,3),
            (1,4),
            (1,5),
            (1,6),
            (2,2),
            (2,4),
            (2,5),
            (2,6),
            (3,3),
            (4,4),
            (4,5),
            (4,6),
            (5,5),
            (6,6),
            (7,7),
            (8,8)
        )

        val outputDf = mapping.execute(executor, Map(MappingOutputIdentifier("input_df") -> inputDf))("main")
        outputDf.schema should be (expectedSchema)
        val outputSeq = outputDf.orderBy("parent_id","child_id").as[(Int,Int)].collect().toSeq
        outputSeq should be (expectedResult)

        val outputSchema = mapping.describe(executor, Map(MappingOutputIdentifier("input_df") -> ftypes.StructType.of(inputDf.schema)))("main")
        outputSchema.sparkType should be (expectedSchema)
    }
}
