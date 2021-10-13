/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.target

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.No
import com.dimajix.flowman.execution.Lifecycle
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Target
import com.dimajix.spark.testing.LocalSparkSession


class NullTargetTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "A NullTarget" should "be parseable" in {
        val spec =
            """
              |targets:
              |  custom:
              |    kind: null
              |    partition:
              |      p1: lala
              |""".stripMargin

        val module = Module.read.string(spec)
        val target = module.targets("custom")
        target shouldBe an[NullTargetSpec]
    }

    it should "do nothing" in {
        val session = Session.builder.withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val target = NullTarget(
            Target.Properties(context),
            Map("p1" -> "lala")
        )

        target.phases should be (Lifecycle.ALL.toSet)
        target.requires(Phase.BUILD) should be (Set())
        target.provides(Phase.BUILD) should be (Set())
        target.before should be (Seq())
        target.after should be (Seq())

        target.dirty(execution, Phase.BUILD) should be (No)
        target.execute(execution, Phase.BUILD)
    }
}
