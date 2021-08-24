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

import scala.collection.immutable.ListMap

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.Yes
import com.dimajix.flowman.execution.ErrorMode
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionTestResult
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Target


class VerifyTargetTest extends AnyFlatSpec with Matchers with MockFactory {
    "The VerifyTarget" should "be parseable" in {
        val spec =
            """
              |targets:
              |  custom:
              |    kind: verify
              |    assertions:
              |      check_primary_key:
              |        kind: sql
              |        tests:
              |          - query: "SELECT * FROM somewhere"
              |            expected: ["a"]
              |""".stripMargin

        val module = Module.read.string(spec)
        val target = module.targets("custom")
        target shouldBe an[VerifyTargetSpec]
    }

    it should "execute assertions" in {
        val session = Session.builder.disableSpark().build()
        val execution = session.execution
        val context = session.context

        val assertion = mock[Assertion]
        val target = VerifyTarget(
            Target.Properties(context),
            Map("a1" -> assertion)
        )

        (assertion.requires _).expects().returns(Set())
        (assertion.inputs _).expects().atLeastOnce().returns(Seq())
        (assertion.name _).expects().returns("a1")
        (assertion.description _).expects().returns(None)
        (assertion.context _).expects().returns(context)
        (assertion.execute _).expects(*,*).returns(Seq(AssertionTestResult("a1", true)))

        target.phases should be (Set(Phase.VERIFY))
        target.requires(Phase.VERIFY) should be (Set())
        target.provides(Phase.VERIFY) should be (Set())
        target.before should be (Seq())
        target.after should be (Seq())

        target.dirty(execution, Phase.VERIFY) should be (Yes)
        target.execute(execution, Phase.VERIFY)
    }

    it should "return success on an empty list of assertions" in {
        val session = Session.builder.disableSpark().build()
        val execution = session.execution
        val context = session.context

        val assertion = mock[Assertion]
        val target = VerifyTarget(
            Target.Properties(context),
            Map("a1" -> assertion)
        )

        (assertion.requires _).expects().returns(Set())
        (assertion.inputs _).expects().atLeastOnce().returns(Seq())
        (assertion.name _).expects().returns("a1")
        (assertion.description _).expects().returns(None)
        (assertion.context _).expects().returns(context)
        (assertion.execute _).expects(*,*).returns(Seq())

        target.phases should be (Set(Phase.VERIFY))
        target.requires(Phase.VERIFY) should be (Set())
        target.provides(Phase.VERIFY) should be (Set())
        target.before should be (Seq())
        target.after should be (Seq())

        target.dirty(execution, Phase.VERIFY) should be (Yes)
        target.execute(execution, Phase.VERIFY)
    }

    it should "throw an exception when an assertion fails" in {
        val session = Session.builder.disableSpark().build()
        val execution = session.execution
        val context = session.context

        val assertion1 = mock[Assertion]
        val assertion2 = mock[Assertion]
        val target = VerifyTarget(
            Target.Properties(context),
            ListMap(
                "a1" -> assertion1,
                "a2" -> assertion2
            )
        )

        (assertion1.requires _).expects().returns(Set())
        (assertion1.inputs _).expects().atLeastOnce().returns(Seq())
        (assertion1.name _).expects().returns("a1")
        (assertion1.description _).expects().returns(None)
        (assertion1.context _).expects().returns(context)
        (assertion1.execute _).expects(*,*).returns(Seq(
            AssertionTestResult("a1", false),
            AssertionTestResult("a1", true)
        ))

        (assertion2.requires _).expects().returns(Set())
        (assertion2.inputs _).expects().atLeastOnce().returns(Seq())
        (assertion2.name _).expects().returns("a2")
        (assertion2.description _).expects().returns(None)
        (assertion2.context _).expects().returns(context)
        (assertion2.execute _).expects(*,*).returns(Seq(
            AssertionTestResult("a3", true)
        ))

        target.phases should be (Set(Phase.VERIFY))
        target.requires(Phase.VERIFY) should be (Set())
        target.provides(Phase.VERIFY) should be (Set())
        target.before should be (Seq())
        target.after should be (Seq())

        target.dirty(execution, Phase.VERIFY) should be (Yes)
        a[VerificationFailedException] should be thrownBy(target.execute(execution, Phase.VERIFY))
    }

    it should "execute all exception if fail_fast is used" in {
        val session = Session.builder.disableSpark().build()
        val execution = session.execution
        val context = session.context

        val assertion1 = mock[Assertion]
        val assertion2 = mock[Assertion]
        val target = VerifyTarget(
            Target.Properties(context),
            ListMap(
                "a1" -> assertion1,
                "a2" -> assertion2
            ),
            errorMode = ErrorMode.FAIL_FAST
        )

        (assertion1.requires _).expects().returns(Set())
        (assertion1.inputs _).expects().atLeastOnce().returns(Seq())
        (assertion1.name _).expects().returns("a1")
        (assertion1.description _).expects().returns(None)
        (assertion1.context _).expects().returns(context)
        (assertion1.execute _).expects(*,*).returns(Seq(
            AssertionTestResult("a1", false),
            AssertionTestResult("a1", true)
        ))

        (assertion2.requires _).expects().returns(Set())
        (assertion2.inputs _).expects().atLeastOnce().returns(Seq())
        (assertion2.name _).expects().returns("a2")
        (assertion2.description _).expects().returns(None)

        target.phases should be (Set(Phase.VERIFY))
        target.requires(Phase.VERIFY) should be (Set())
        target.provides(Phase.VERIFY) should be (Set())
        target.before should be (Seq())
        target.after should be (Seq())

        target.dirty(execution, Phase.VERIFY) should be (Yes)
        a[VerificationFailedException] should be thrownBy(target.execute(execution, Phase.VERIFY))
    }

    it should "not throw an exception if fail_never is used" in {
        val session = Session.builder.disableSpark().build()
        val execution = session.execution
        val context = session.context

        val assertion1 = mock[Assertion]
        val assertion2 = mock[Assertion]
        val target = VerifyTarget(
            Target.Properties(context),
            ListMap(
                "a1" -> assertion1,
                "a2" -> assertion2
            ),
            errorMode = ErrorMode.FAIL_NEVER
        )

        (assertion1.requires _).expects().returns(Set())
        (assertion1.inputs _).expects().atLeastOnce().returns(Seq())
        (assertion1.name _).expects().returns("a1")
        (assertion1.description _).expects().returns(None)
        (assertion1.context _).expects().returns(context)
        (assertion1.execute _).expects(*,*).returns(Seq(
            AssertionTestResult("a1", false),
            AssertionTestResult("a1", true)
        ))

        (assertion2.requires _).expects().returns(Set())
        (assertion2.inputs _).expects().atLeastOnce().returns(Seq())
        (assertion2.name _).expects().returns("a2")
        (assertion2.description _).expects().returns(None)
        (assertion2.context _).expects().returns(context)
        (assertion2.execute _).expects(*,*).returns(Seq(
            AssertionTestResult("a3", true)
        ))

        target.phases should be (Set(Phase.VERIFY))
        target.requires(Phase.VERIFY) should be (Set())
        target.provides(Phase.VERIFY) should be (Set())
        target.before should be (Seq())
        target.after should be (Seq())

        target.dirty(execution, Phase.VERIFY) should be (Yes)
        noException should be thrownBy(target.execute(execution, Phase.VERIFY))
    }
}
