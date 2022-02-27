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


class ColumnCheckTest extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession {
    "A NotNullColumnCheck" should "be executable" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val context = session.context
        val testExecutor = new DefaultColumnCheckExecutor

        val df = spark.createDataFrame(Seq((Some(1),2), (None,3)))

        val test = NotNullColumnCheck(None)
        val result1 = testExecutor.execute(execution, context, df, "_1", test)
        result1 should be (Some(CheckResult(Some(test.reference), CheckStatus.FAILED, description=Some("1 records passed, 1 records failed"))))
        val result2 = testExecutor.execute(execution, context, df, "_2", test)
        result2 should be (Some(CheckResult(Some(test.reference), CheckStatus.SUCCESS, description=Some("2 records passed, 0 records failed"))))
        an[Exception] should be thrownBy(testExecutor.execute(execution, context, df, "_3", test))
    }

    "A UniqueColumnCheck" should "be executable" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val context = session.context
        val testExecutor = new DefaultColumnCheckExecutor

        val df = spark.createDataFrame(Seq(
            (Some(1),2,3),
            (None,3,4),
            (None,3,5)
        ))

        val test = UniqueColumnCheck(None)
        val result1 = testExecutor.execute(execution, context, df, "_1", test)
        result1 should be (Some(CheckResult(Some(test.reference), CheckStatus.SUCCESS, description=Some("1 values are unique, 0 values are non-unique"))))
        val result2 = testExecutor.execute(execution, context, df, "_2", test)
        result2 should be (Some(CheckResult(Some(test.reference), CheckStatus.FAILED, description=Some("1 values are unique, 1 values are non-unique"))))
        val result3 = testExecutor.execute(execution, context, df, "_3", test)
        result3 should be (Some(CheckResult(Some(test.reference), CheckStatus.SUCCESS, description=Some("3 values are unique, 0 values are non-unique"))))
        an[Exception] should be thrownBy(testExecutor.execute(execution, context, df, "_4", test))
    }

    "A ValuesColumnCheck" should "be executable" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val context = session.context
        val testExecutor = new DefaultColumnCheckExecutor

        val df = spark.createDataFrame(Seq(
            (Some(1),2,1),
            (None,3,2)
        ))

        val test = ValuesColumnCheck(None, values=Seq(1,2))
        val result1 = testExecutor.execute(execution, context, df, "_1", test)
        result1 should be (Some(CheckResult(Some(test.reference), CheckStatus.SUCCESS, description=Some("1 records passed, 0 records failed"))))
        val result2 = testExecutor.execute(execution, context, df, "_2", test)
        result2 should be (Some(CheckResult(Some(test.reference), CheckStatus.FAILED, description=Some("1 records passed, 1 records failed"))))
        val result3 = testExecutor.execute(execution, context, df, "_3", test)
        result3 should be (Some(CheckResult(Some(test.reference), CheckStatus.SUCCESS, description=Some("2 records passed, 0 records failed"))))
        an[Exception] should be thrownBy(testExecutor.execute(execution, context, df, "_4", test))
    }

    it should "use correct data types" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val context = session.context
        val testExecutor = new DefaultColumnCheckExecutor

        val df = spark.createDataFrame(Seq(
            (Some(1),2,1),
            (None,3,2)
        ))

        val test = ValuesColumnCheck(None, values=Seq(1,2))
        val result1 = testExecutor.execute(execution, context, df, "_1", test)
        result1 should be (Some(CheckResult(Some(test.reference), CheckStatus.SUCCESS, description=Some("1 records passed, 0 records failed"))))
        val result2 = testExecutor.execute(execution, context, df, "_2", test)
        result2 should be (Some(CheckResult(Some(test.reference), CheckStatus.FAILED, description=Some("1 records passed, 1 records failed"))))
        val result3 = testExecutor.execute(execution, context, df, "_3", test)
        result3 should be (Some(CheckResult(Some(test.reference), CheckStatus.SUCCESS, description=Some("2 records passed, 0 records failed"))))
        an[Exception] should be thrownBy(testExecutor.execute(execution, context, df, "_4", test))
    }

    "A RangeColumnCheck" should "be executable" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val context = session.context
        val testExecutor = new DefaultColumnCheckExecutor

        val df = spark.createDataFrame(Seq(
            (Some(1),2,1),
            (None,3,2)
        ))

        val test = RangeColumnCheck(None, lower=1, upper=2)
        val result1 = testExecutor.execute(execution, context, df, "_1", test)
        result1 should be (Some(CheckResult(Some(test.reference), CheckStatus.SUCCESS, description=Some("1 records passed, 0 records failed"))))
        val result2 = testExecutor.execute(execution, context, df, "_2", test)
        result2 should be (Some(CheckResult(Some(test.reference), CheckStatus.FAILED, description=Some("1 records passed, 1 records failed"))))
        val result3 = testExecutor.execute(execution, context, df, "_3", test)
        result3 should be (Some(CheckResult(Some(test.reference), CheckStatus.SUCCESS, description=Some("2 records passed, 0 records failed"))))
        an[Exception] should be thrownBy(testExecutor.execute(execution, context, df, "_4", test))
    }

    it should "use correct data types" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val context = session.context
        val testExecutor = new DefaultColumnCheckExecutor

        val df = spark.createDataFrame(Seq(
            (Some(1),2,1),
            (None,3,2)
        ))

        val test = RangeColumnCheck(None, lower="1.0", upper="2.2")
        val result1 = testExecutor.execute(execution, context, df, "_1", test)
        result1 should be (Some(CheckResult(Some(test.reference), CheckStatus.SUCCESS, description=Some("1 records passed, 0 records failed"))))
        val result2 = testExecutor.execute(execution, context, df, "_2", test)
        result2 should be (Some(CheckResult(Some(test.reference), CheckStatus.FAILED, description=Some("1 records passed, 1 records failed"))))
        val result3 = testExecutor.execute(execution, context, df, "_3", test)
        result3 should be (Some(CheckResult(Some(test.reference), CheckStatus.SUCCESS, description=Some("2 records passed, 0 records failed"))))
    }

    "An ExpressionColumnCheck" should "succeed" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val context = session.context
        val testExecutor = new DefaultColumnCheckExecutor

        val df = spark.createDataFrame(Seq(
            (Some(1),2,1),
            (None,3,2)
        ))

        val test = ExpressionColumnCheck(None, expression="_2 > _3")
        val result1 = testExecutor.execute(execution, context, df, "_1", test)
        result1 should be (Some(CheckResult(Some(test.reference), CheckStatus.SUCCESS, description=Some("2 records passed, 0 records failed"))))
        val result2 = testExecutor.execute(execution, context, df, "_2", test)
        result2 should be (Some(CheckResult(Some(test.reference), CheckStatus.SUCCESS, description=Some("2 records passed, 0 records failed"))))
        val result4 = testExecutor.execute(execution, context, df, "_4", test)
        result4 should be (Some(CheckResult(Some(test.reference), CheckStatus.SUCCESS, description=Some("2 records passed, 0 records failed"))))
    }

    it should "fail" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val context = session.context
        val testExecutor = new DefaultColumnCheckExecutor

        val df = spark.createDataFrame(Seq(
            (Some(1),2,1),
            (None,3,2)
        ))

        val test = ExpressionColumnCheck(None, expression="_2 < _3")
        val result1 = testExecutor.execute(execution, context, df, "_1", test)
        result1 should be (Some(CheckResult(Some(test.reference), CheckStatus.FAILED, description=Some("0 records passed, 2 records failed"))))
        val result2 = testExecutor.execute(execution, context, df, "_2", test)
        result2 should be (Some(CheckResult(Some(test.reference), CheckStatus.FAILED, description=Some("0 records passed, 2 records failed"))))
        val result4 = testExecutor.execute(execution, context, df, "_4", test)
        result4 should be (Some(CheckResult(Some(test.reference), CheckStatus.FAILED, description=Some("0 records passed, 2 records failed"))))
    }

    "A ForeignKeyColumnCheck" should "work" in {
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

        val testExecutor = new DefaultColumnCheckExecutor

        val df = spark.createDataFrame(Seq(
            (Some(1),1,1),
            (None,2,3)
        ))
        val otherDf = spark.createDataFrame(Seq(
            (1,1),
            (2,2)
        ))

        (mappingSpec.instantiate _).expects(*).returns(mapping)
        (mapping.context _).expects().returns(context)
        (mapping.inputs _).expects().returns(Set())
        (mapping.outputs _).expects().atLeastOnce().returns(Set("main"))
        (mapping.broadcast _).expects().returns(false)
        (mapping.cache _).expects().returns(StorageLevel.NONE)
        (mapping.checkpoint _).expects().returns(false)
        (mapping.identifier _).expects().returns(MappingIdentifier("project/mapping"))
        (mapping.execute _).expects(*,*).returns(Map("main" -> otherDf))

        val test = ForeignKeyColumnCheck(None, mapping=Some(MappingOutputIdentifier("mapping")), column=Some("_1"))
        val result1 = testExecutor.execute(execution, context, df, "_1", test)
        result1 should be (Some(CheckResult(Some(test.reference), CheckStatus.SUCCESS, description=Some("1 records passed, 0 records failed"))))
        val result2 = testExecutor.execute(execution, context, df, "_2", test)
        result2 should be (Some(CheckResult(Some(test.reference), CheckStatus.SUCCESS, description=Some("2 records passed, 0 records failed"))))
        val result3 = testExecutor.execute(execution, context, df, "_3", test)
        result3 should be (Some(CheckResult(Some(test.reference), CheckStatus.FAILED, description=Some("1 records passed, 1 records failed"))))
        an[Exception] should be thrownBy(testExecutor.execute(execution, context, df, "_4", test))
    }
}
