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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.spark.testing.LocalSparkSession


class ColumnTestTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "A NotNullColumnTest" should "be executable" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val testExecutor = new DefaultColumnTestExecutor

        val df = spark.createDataFrame(Seq((Some(1),2), (None,3)))

        val test = NotNullColumnTest(None)
        val result1 = testExecutor.execute(execution, df, "_1", test)
        result1 should be (Some(TestResult(Some(test.reference), TestStatus.FAILED)))
        val result2 = testExecutor.execute(execution, df, "_2", test)
        result2 should be (Some(TestResult(Some(test.reference), TestStatus.SUCCESS)))
        an[Exception] should be thrownBy(testExecutor.execute(execution, df, "_3", test))
    }

    "A UniqueColumnTest" should "be executable" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val execution = session.execution
        val testExecutor = new DefaultColumnTestExecutor

        val df = spark.createDataFrame(Seq(
            (Some(1),2,3),
            (None,3,4),
            (None,3,5)
        ))

        val test = UniqueColumnTest(None)
        val result1 = testExecutor.execute(execution, df, "_1", test)
        result1 should be (Some(TestResult(Some(test.reference), TestStatus.SUCCESS)))
        val result2 = testExecutor.execute(execution, df, "_2", test)
        result2 should be (Some(TestResult(Some(test.reference), TestStatus.FAILED)))
        val result3 = testExecutor.execute(execution, df, "_3", test)
        result3 should be (Some(TestResult(Some(test.reference), TestStatus.SUCCESS)))
        an[Exception] should be thrownBy(testExecutor.execute(execution, df, "_4", test))
    }
}
