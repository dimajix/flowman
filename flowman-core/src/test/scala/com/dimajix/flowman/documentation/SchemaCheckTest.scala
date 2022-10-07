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

package com.dimajix.flowman.documentation

import org.apache.spark.storage.StorageLevel
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Prototype
import com.dimajix.spark.testing.LocalSparkSession


class SchemaCheckTest extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession {
    "A PrimaryKeySchemaCheck" should "be executable" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val context = session.context
        val testExecutor = new DefaultSchemaCheckExecutor

        val df = spark.createDataFrame(Seq(
            (Some(1),2,3),
            (None,3,4),
            (None,3,5)
        ))

        val test1 = PrimaryKeySchemaCheck(None, columns=Seq("_1","_3"))
        val result1 = testExecutor.execute(execution, context, df, test1)
        result1 should be (Some(CheckResult(Some(test1.reference), CheckStatus.SUCCESS, description=Some("3 keys are unique, 0 keys are non-unique"))))

        val test2 = PrimaryKeySchemaCheck(None, columns=Seq("_1","_2"))
        val result2 = testExecutor.execute(execution, context, df, test2)
        result2 should be (Some(CheckResult(Some(test1.reference), CheckStatus.FAILED, description=Some("1 keys are unique, 1 keys are non-unique"))))
    }

    "An ExpressionSchemaCheck" should "work" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val context = session.context
        val testExecutor = new DefaultSchemaCheckExecutor

        val df = spark.createDataFrame(Seq(
            (Some(1),2,1),
            (None,3,2)
        ))

        val test1 = ExpressionSchemaCheck(None, expression="_2 > _3")
        val result1 = testExecutor.execute(execution, context, df, test1)
        result1 should be (Some(CheckResult(Some(test1.reference), CheckStatus.SUCCESS, description=Some("2 records passed, 0 records failed"))))

        val test2 = ExpressionSchemaCheck(None, expression="_2 < _3")
        val result2 = testExecutor.execute(execution, context, df, test2)
        result2 should be (Some(CheckResult(Some(test1.reference), CheckStatus.FAILED, description=Some("0 records passed, 2 records failed"))))
    }

    it should "support filter conditions" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()

        val excludes = spark.createDataFrame(Seq(
            (3,3)
        ))
        val df = spark.createDataFrame(Seq(
            (Some(1),2,1),
            (None,3,2)
        ))

        val valuesGen = mock[Prototype[Mapping]]
        val valuesMapping = mock[Mapping]
        val project = Project("project",
            mappings = Map("excludes" -> valuesGen)
        )

        val execution = session.execution
        val context = session.getContext(project)

        (valuesGen.instantiate _).expects(*,*).returns(valuesMapping)
        (valuesMapping.identifier _).expects().atLeastOnce().returns(MappingIdentifier("excludes"))
        (valuesMapping.output _).expects().atLeastOnce().returns(MappingOutputIdentifier("excludes"))
        (valuesMapping.outputs _).expects().atLeastOnce().returns(Set("main"))
        (valuesMapping.inputs _).expects().returns(Set.empty)
        (valuesMapping.context _).expects().returns(context)
        (valuesMapping.broadcast _).expects().returns(false)
        (valuesMapping.cache _).expects().returns(StorageLevel.NONE)
        (valuesMapping.checkpoint _).expects().returns(false)
        (valuesMapping.execute _).expects(*,*).returns(Map("main" -> excludes))

        val testExecutor = new DefaultSchemaCheckExecutor
        val test1 = ExpressionSchemaCheck(None, expression="_2 > _3", filter=Some("_2 NOT IN (SELECT _1 FROM excludes)"))
        val result1 = testExecutor.execute(execution, context, df, test1)
        result1 should be (Some(CheckResult(Some(test1.reference), CheckStatus.SUCCESS, description=Some("1 records passed, 0 records failed"))))
    }

    it should "work with null values" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val context = session.context
        val testExecutor = new DefaultSchemaCheckExecutor

        val df = spark.createDataFrame(Seq(
            (Some(1), 2),
            (None, 3)
        ))

        val test1 = ExpressionSchemaCheck(None, expression = "_1 > _2")
        val result1 = testExecutor.execute(execution, context, df, test1)
        result1 should be(Some(CheckResult(Some(test1.reference), CheckStatus.FAILED, description = Some("0 records passed, 2 records failed"))))

        val test2 = ExpressionSchemaCheck(None, expression = "_1 < _2")
        val result2 = testExecutor.execute(execution, context, df, test2)
        result2 should be(Some(CheckResult(Some(test1.reference), CheckStatus.FAILED, description = Some("1 records passed, 1 records failed"))))
    }

    "A ForeignKeySchemaCheck" should "work" in {
        val mappingSpec = mock[Prototype[Mapping]]
        val mapping = mock[Mapping]

        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val project = Project(
            name = "project",
            mappings = Map("mapping" -> mappingSpec)
        )
        val context = session.getContext(project)
        val execution = session.execution

        val testExecutor = new DefaultSchemaCheckExecutor

        val df = spark.createDataFrame(Seq(
            (Some(1),1,1),
            (None,2,3)
        ))
        val otherDf = spark.createDataFrame(Seq(
            (1,1),
            (1,1),
            (2,2),
            (2,2)
        ))

        (mappingSpec.instantiate _).expects(*,None).returns(mapping)
        (mapping.context _).expects().returns(context)
        (mapping.inputs _).expects().returns(Set())
        (mapping.outputs _).expects().atLeastOnce().returns(Set("main"))
        (mapping.broadcast _).expects().returns(false)
        (mapping.cache _).expects().returns(StorageLevel.NONE)
        (mapping.checkpoint _).expects().returns(false)
        (mapping.identifier _).expects().returns(MappingIdentifier("project/mapping"))
        (mapping.execute _).expects(*,*).returns(Map("main" -> otherDf))

        val test1 = ForeignKeySchemaCheck(None, mapping=Some(MappingOutputIdentifier("mapping")), columns=Seq("_1"))
        val result1 = testExecutor.execute(execution, context, df, test1)
        result1 should be (Some(CheckResult(Some(test1.reference), CheckStatus.SUCCESS, description=Some("1 records passed, 0 records failed"))))

        val test2 = ForeignKeySchemaCheck(None, mapping=Some(MappingOutputIdentifier("mapping")), columns=Seq("_3"), references=Seq("_2"))
        val result2 = testExecutor.execute(execution, context, df, test2)
        result2 should be (Some(CheckResult(Some(test1.reference), CheckStatus.FAILED, description=Some("1 records passed, 1 records failed"))))

        val test3 = ForeignKeySchemaCheck(None, mapping=Some(MappingOutputIdentifier("mapping")), columns=Seq("_2"))
        val result3 = testExecutor.execute(execution, context, df, test3)
        result3 should be (Some(CheckResult(Some(test3.reference), CheckStatus.SUCCESS, description=Some("2 records passed, 0 records failed"))))

        val test4 = ForeignKeySchemaCheck(None, mapping=Some(MappingOutputIdentifier("mapping")), columns=Seq("_2"), references=Seq("_3"))
        an[Exception] should be thrownBy(testExecutor.execute(execution, context, df, test4))
    }

    "An SqlSchemaCheck" should "work with grouped counts" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val context = session.context
        val testExecutor = new DefaultSchemaCheckExecutor

        val df = spark.createDataFrame(Seq(
            (Some(1),2,1),
            (None,3,2)
        ))

        val test1 = SqlSchemaCheck(None, query="SELECT _2 > _3, 1 FROM __this__")
        val result1 = testExecutor.execute(execution, context, df, test1)
        result1 should be (Some(CheckResult(Some(test1.reference), CheckStatus.SUCCESS, description=Some("2 records passed, 0 records failed"))))

        val test2 = SqlSchemaCheck(None, query="SELECT _2 < _3, 1 FROM __THIS__")
        val result2 = testExecutor.execute(execution, context, df, test2)
        result2 should be (Some(CheckResult(Some(test1.reference), CheckStatus.FAILED, description=Some("0 records passed, 2 records failed"))))
    }

    it should "work with one-line results" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val context = session.context
        val testExecutor = new DefaultSchemaCheckExecutor

        val df = spark.createDataFrame(Seq(
            (Some(3),2,1),
            (None,3,2)
        ))

        val test1 = SqlSchemaCheck(None, query=
            """
              |SELECT
              | (SELECT SUM(_2) FROM __this__) AS sum_2,
              | (SELECT SUM(_3) FROM __this__) AS sum_3,
              | (SELECT SUM(_2) FROM __this__) = (SELECT SUM(_3) FROM __this__) AS success""".stripMargin)
        val result1 = testExecutor.execute(execution, context, df, test1)
        result1 should be (Some(CheckResult(Some(test1.reference), CheckStatus.FAILED, description=Some("sum_2=5, sum_3=3"))))

        val test2 = SqlSchemaCheck(None, query=
            """
              |SELECT
              | (SELECT SUM(_1) FROM __this__) AS sum_1,
              | (SELECT SUM(_3) FROM __this__) AS sum_3,
              | (SELECT SUM(_1) FROM __this__) = (SELECT SUM(_3) FROM __this__) AS success""".stripMargin)
        val result2 = testExecutor.execute(execution, context, df, test2)
        result2 should be (Some(CheckResult(Some(test1.reference), CheckStatus.SUCCESS, description=Some("sum_1=3, sum_3=3"))))
    }

    it should "throw an exception for unsupported queries" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val context = session.context
        val testExecutor = new DefaultSchemaCheckExecutor

        val df = spark.createDataFrame(Seq(
            (Some(1), 2, 1),
            (None, 3, 2)
        ))

        val test1 = SqlSchemaCheck(None, query = "SELECT _2 > _3 FROM __this__")
        an[IllegalArgumentException] should be thrownBy(testExecutor.execute(execution, context, df, test1))

        val test2 = SqlSchemaCheck(None, query = "SELECT _2 > _3, TRUE, FALSE FROM __this__")
        an[IllegalArgumentException] should be thrownBy (testExecutor.execute(execution, context, df, test2))
    }
}
