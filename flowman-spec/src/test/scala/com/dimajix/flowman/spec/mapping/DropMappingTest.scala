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

package com.dimajix.flowman.spec.mapping

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.transforms.schema.Path
import com.dimajix.flowman.{types => ftypes}
import com.dimajix.spark.testing.LocalSparkSession


class DropMappingTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The DropMapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  drop:
              |    kind: drop
              |    input: dummy
              |    columns:
              |      - col_1
              |      - col-2
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        project.mappings.keys should contain("drop")
        project.mappings("drop") shouldBe a[DropMappingSpec]

        val session = Session.builder(project).disableSpark().build()
        val context = session.getContext(project)
        val instance = context.getMapping(MappingIdentifier("drop"))
        instance shouldBe an[DropMapping]

        session.shutdown()
    }

    it should "drop known columns" in {
        val sparkSession = spark
        import sparkSession.implicits._

        val records = Seq(
            ("row_1", "col2", "col3", "col4")
        )
        val inputDf = records.toDF
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val mapping = DropMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("df"),
            Seq(Path("_1"), Path("_3"))
        )
        val expectedSchema = StructType(Seq(
            StructField("_2", StringType),
            StructField("_4", StringType)
        ))

        val outputDf = mapping.execute(executor, Map(MappingOutputIdentifier("df") -> inputDf))("main")
        outputDf.schema should be (expectedSchema)

        val outputSchema = mapping.describe(executor, Map(MappingOutputIdentifier("df") -> ftypes.StructType.of(inputDf.schema)))("main")
        outputSchema.sparkType should be (expectedSchema)

        session.shutdown()
    }

    it should "ignore non-existing columns" in {
        val sparkSession = spark
        import sparkSession.implicits._

        val records = Seq(
            ("row_1", "col2", "col3", "col4")
        )
        val inputDf = records.toDF
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val mapping = DropMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("df"),
            Seq(Path("_1"), Path("_3"), Path("no_such_column"))
        )
        val expectedSchema = StructType(Seq(
            StructField("_2", StringType),
            StructField("_4", StringType)
        ))

        val outputDf = mapping.execute(executor, Map(MappingOutputIdentifier("df") -> inputDf))("main")
        outputDf.schema should be (expectedSchema)

        val outputSchema = mapping.describe(executor, Map(MappingOutputIdentifier("df") -> ftypes.StructType.of(inputDf.schema)))("main")
        outputSchema.sparkType should be (expectedSchema)

        session.shutdown()
    }
}
