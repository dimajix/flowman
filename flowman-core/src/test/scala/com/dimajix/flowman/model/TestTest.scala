/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.model

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session


class TestTest extends AnyFlatSpec with Matchers with MockFactory {
    "Test.merge" should "correctly merge Tests" in {
        val session = Session.builder().disableSpark().build()
        val context = session.context

        val test = Test.builder(context)
            .setProperties(Test.Properties(context, "some_test"))
            .setDescription("Some test")
            .setEnvironment(Map("env1" -> "eval_1", "env2" -> "eval_2", "p2" -> "17"))
            .setTargets(Seq(TargetIdentifier("t2"),TargetIdentifier("t7"),TargetIdentifier("t1"),TargetIdentifier("t3")))
            .build()
        val parent = Test.builder(context)
            .setProperties(Test.Properties(context, "parent_test"))
            .setDescription("Some parent test")
            .setEnvironment(Map("env1" -> "parent_val_1", "env4" -> "parent_val_4"))
            .setTargets(Seq(TargetIdentifier("t3"),TargetIdentifier("t4"),TargetIdentifier("t6"),TargetIdentifier("t5")))
            .build()

        val result = Test.merge(test, Seq(parent))
        result.name should be ("some_test")
        result.description should be (Some("Some test"))
        result.targets should be (Seq(
            TargetIdentifier("t3"),
            TargetIdentifier("t4"),
            TargetIdentifier("t6"),
            TargetIdentifier("t5"),
            TargetIdentifier("t2"),
            TargetIdentifier("t7"),
            TargetIdentifier("t1")
        ))
        result.environment should be (Map(
            "env1" -> "eval_1",
            "env2" -> "eval_2",
            "env4" -> "parent_val_4",
            "p2" -> "17"
        ))

        session.shutdown()
    }
}
