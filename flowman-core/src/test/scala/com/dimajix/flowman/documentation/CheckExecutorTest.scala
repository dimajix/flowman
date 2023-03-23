/*
 * Copyright (C) 2022 The Flowman Authors
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

import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.spark.testing.LocalSparkSession


class CheckExecutorTest extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession {
    "A CheckExecutor" should "work in non-parallel mode" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .withConfig(FlowmanConf.EXECUTION_CHECK_PARALLELISM.key, "1")
            .build()
        val context = session.context
        val execution = session.execution

        val df = spark.createDataFrame(Seq((Some(1), 2), (None, 3)))
        val mapping = mock[Mapping]
        (mapping.identifier _).expects().atLeastOnce().returns(MappingIdentifier("mapping"))
        (mapping.context _).expects().atLeastOnce().returns(context)
        (mapping.inputs _).expects().returns(Set.empty)
        (mapping.outputs _).expects().atLeastOnce().returns(Set("main"))
        (mapping.broadcast _).expects().returns(false)
        (mapping.checkpoint _).expects().returns(false)
        (mapping.cache _).expects().returns(StorageLevel.NONE)
        (mapping.execute _).expects(*,*).returns(Map("main" -> df))

        val doc = MappingDoc(
            mapping = Some(mapping),
            outputs = Seq(
                MappingOutputDoc(
                    Some(MappingReference(None, "mapping")),
                    MappingOutputIdentifier("mapping:main"),
                    schema = Some(SchemaDoc(
                        None,
                        checks = Seq(
                            PrimaryKeySchemaCheck(None, columns = Seq("_1")),
                            PrimaryKeySchemaCheck(None, columns = Seq("_2")),
                            PrimaryKeySchemaCheck(None, columns = Seq("_3")),
                            PrimaryKeySchemaCheck(None, columns = Seq("_1","_2"))
                        )
                    ))
                )
            )
        )

        val checkExecutor = new CheckExecutor(execution)
        val checkResult = checkExecutor.executeTests(mapping, doc)
        checkResult.outputs.map(_.schema.get).flatMap(_.checks).flatMap(_.result).map(_.status) should be (Seq(
            CheckStatus.SUCCESS,
            CheckStatus.SUCCESS,
            CheckStatus.ERROR,
            CheckStatus.SUCCESS
        ))

        session.shutdown()
    }

    it should "work in parallel mode" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .withConfig(FlowmanConf.EXECUTION_CHECK_PARALLELISM.key, "4")
            .build()
        val context = session.context
        val execution = session.execution

        val df = spark.createDataFrame(Seq((Some(1), 2), (None, 3)))
        val mapping = mock[Mapping]
        (mapping.identifier _).expects().atLeastOnce().returns(MappingIdentifier("mapping"))
        (mapping.context _).expects().atLeastOnce().returns(context)
        (mapping.inputs _).expects().returns(Set.empty)
        (mapping.outputs _).expects().atLeastOnce().returns(Set("main"))
        (mapping.broadcast _).expects().returns(false)
        (mapping.checkpoint _).expects().returns(false)
        (mapping.cache _).expects().returns(StorageLevel.NONE)
        (mapping.execute _).expects(*, *).returns(Map("main" -> df))

        val doc = MappingDoc(
            mapping = Some(mapping),
            outputs = Seq(
                MappingOutputDoc(
                    Some(MappingReference(None, "mapping")),
                    MappingOutputIdentifier("mapping:main"),
                    schema = Some(SchemaDoc(
                        None,
                        checks = Seq(
                            PrimaryKeySchemaCheck(None, columns = Seq("_1")),
                            PrimaryKeySchemaCheck(None, columns = Seq("_2")),
                            PrimaryKeySchemaCheck(None, columns = Seq("_3")),
                            PrimaryKeySchemaCheck(None, columns = Seq("_1", "_2"))
                        )
                    ))
                )
            )
        )

        val checkExecutor = new CheckExecutor(execution)
        val checkResult = checkExecutor.executeTests(mapping, doc)
        checkResult.outputs.map(_.schema.get).flatMap(_.checks).flatMap(_.result).map(_.status) should be(Seq(
            CheckStatus.SUCCESS,
            CheckStatus.SUCCESS,
            CheckStatus.ERROR,
            CheckStatus.SUCCESS
        ))

        session.shutdown()
    }
}
