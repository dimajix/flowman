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

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.ValidationFailedException
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Target


class ValidateTargetTest extends AnyFlatSpec with Matchers with MockFactory {
    "The ValidateTarget" should "be parseable" in {
        val spec =
            """
              |targets:
              |  custom:
              |    kind: validate
              |    assertions:
              |      check_primary_key:
              |        kind: sql
              |        tests:
              |          - query: "SELECT * FROM somewhere"
              |            expected: ["a"]
              |""".stripMargin

        val module = Module.read.string(spec)
        val target = module.targets("custom")
        target shouldBe an[ValidateTargetSpec]
    }

    it should "execute assertions" in {
        val session = Session.builder.build()
        val execution = session.execution
        val context = session.context

        val assertion = mock[Assertion]
        val target = ValidateTarget(
            Target.Properties(context),
            Map("a1" -> assertion)
        )

        (assertion.requires _).expects().returns(Set())
        (assertion.inputs _).expects().atLeastOnce().returns(Seq())
        (assertion.description _).expects().returns(None)
        (assertion.context _).expects().returns(context)
        (assertion.execute _).expects(*,*).returns(Seq(AssertionResult("a1", true)))

        target.phases should be (Set(Phase.VALIDATE))
        target.requires(Phase.VALIDATE) should be (Set())
        target.provides(Phase.VALIDATE) should be (Set())
        target.before should be (Seq())
        target.after should be (Seq())

        target.dirty(execution, Phase.VALIDATE) should be (Yes)
        target.execute(execution, Phase.VALIDATE)
    }

    it should "return success on an empty list of assertions" in {
        val session = Session.builder.build()
        val execution = session.execution
        val context = session.context

        val assertion = mock[Assertion]
        val target = ValidateTarget(
            Target.Properties(context),
            Map("a1" -> assertion)
        )

        (assertion.requires _).expects().returns(Set())
        (assertion.inputs _).expects().atLeastOnce().returns(Seq())
        (assertion.description _).expects().returns(None)
        (assertion.context _).expects().returns(context)
        (assertion.execute _).expects(*,*).returns(Seq())

        target.phases should be (Set(Phase.VALIDATE))
        target.requires(Phase.VALIDATE) should be (Set())
        target.provides(Phase.VALIDATE) should be (Set())
        target.before should be (Seq())
        target.after should be (Seq())

        target.dirty(execution, Phase.VALIDATE) should be (Yes)
        target.execute(execution, Phase.VALIDATE)
    }

    it should "throw an exception when an assertion fails" in {
        val session = Session.builder.build()
        val execution = session.execution
        val context = session.context

        val assertion = mock[Assertion]
        val target = ValidateTarget(
            Target.Properties(context),
            Map("a1" -> assertion)
        )

        (assertion.requires _).expects().returns(Set())
        (assertion.inputs _).expects().atLeastOnce().returns(Seq())
        (assertion.description _).expects().returns(None)
        (assertion.context _).expects().returns(context)
        (assertion.execute _).expects(*,*).returns(Seq(
            AssertionResult("a1", false),
            AssertionResult("a1", true)
        ))

        target.phases should be (Set(Phase.VALIDATE))
        target.requires(Phase.VALIDATE) should be (Set())
        target.provides(Phase.VALIDATE) should be (Set())
        target.before should be (Seq())
        target.after should be (Seq())

        target.dirty(execution, Phase.VALIDATE) should be (Yes)
        a[ValidationFailedException] should be thrownBy(target.execute(execution, Phase.VALIDATE))
    }
}
