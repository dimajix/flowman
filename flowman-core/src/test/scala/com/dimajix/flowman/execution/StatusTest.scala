/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class StatusTest extends AnyFlatSpec with Matchers {
    "Status.ofAll" should "return SUCCESS for empty lists" in {
        Status.ofAll(Seq()) should be (Status.SUCCESS)
    }
    it should "return SUCCESS if all succeed" in {
        Status.ofAll(Seq(Status.SUCCESS, Status.SUCCESS)) should be (Status.SUCCESS)
    }
    it should "return SUCCESS_WITH_ERRORS if some succeed with error" in {
        Status.ofAll(Seq(Status.SUCCESS, Status.SUCCESS_WITH_ERRORS)) should be (Status.SUCCESS_WITH_ERRORS)
    }
    it should "return FAILED for any failure" in {
        Status.ofAll(Seq(Status.SKIPPED, Status.FAILED, Status.SUCCESS, Status.SUCCESS_WITH_ERRORS)) should be (Status.FAILED)
    }
    it should "return FAILED for any UNKNOWN" in {
        Status.ofAll(Seq(Status.SKIPPED, Status.UNKNOWN, Status.SUCCESS, Status.SUCCESS_WITH_ERRORS)) should be (Status.FAILED)
    }
    it should "return SKIPPED if all are skipped" in {
        Status.ofAll(Seq(Status.SKIPPED, Status.SKIPPED, Status.SUCCESS)) should be (Status.SUCCESS)
        Status.ofAll(Seq(Status.SKIPPED, Status.SKIPPED, Status.SUCCESS_WITH_ERRORS)) should be (Status.SUCCESS_WITH_ERRORS)
        Status.ofAll(Seq(Status.SKIPPED, Status.SKIPPED)) should be (Status.SKIPPED)
    }

    "Status.parallelOfAll" should "return SUCCESS for empty lists" in {
        Status.parallelOfAll(Seq(),4)(identity) should be (Status.SUCCESS)
    }
    it should "return SUCCESS if all succeed" in {
        Status.parallelOfAll(Seq(Status.SUCCESS, Status.SUCCESS),4)(identity) should be (Status.SUCCESS)
    }
    it should "return SUCCESS_WITH_ERRORS if some succeed with error" in {
        Status.parallelOfAll(Seq(Status.SUCCESS, Status.SUCCESS_WITH_ERRORS),4)(identity) should be (Status.SUCCESS_WITH_ERRORS)
    }
    it should "return FAILED for any failure" in {
        Status.parallelOfAll(Seq(Status.SKIPPED, Status.FAILED, Status.SUCCESS, Status.SUCCESS_WITH_ERRORS),4)(identity) should be (Status.FAILED)
    }
    it should "return FAILED for any UNKNOWN" in {
        Status.parallelOfAll(Seq(Status.SKIPPED, Status.UNKNOWN, Status.SUCCESS, Status.SUCCESS_WITH_ERRORS),4)(identity) should be (Status.FAILED)
    }
    it should "return SKIPPED if all are skipped" in {
        Status.parallelOfAll(Seq(Status.SKIPPED, Status.SKIPPED, Status.SUCCESS),4)(identity) should be (Status.SUCCESS)
        Status.parallelOfAll(Seq(Status.SKIPPED, Status.SKIPPED, Status.SUCCESS_WITH_ERRORS),4)(identity) should be (Status.SUCCESS_WITH_ERRORS)
        Status.parallelOfAll(Seq(Status.SKIPPED, Status.SKIPPED),4)(identity) should be (Status.SKIPPED)
    }

    "Status.success and Status.failure" should "work correctly" in {
        Status.UNKNOWN.success should be (false)
        Status.RUNNING.success should be (false)
        Status.SUCCESS.success should be (true)
        Status.SUCCESS_WITH_ERRORS.success should be (true)
        Status.FAILED.success should be (false)
        Status.ABORTED.success should be (false)
        Status.SKIPPED.success should be (true)

        Status.UNKNOWN.failure should be (true)
        Status.RUNNING.failure should be (true)
        Status.SUCCESS.failure should be (false)
        Status.SUCCESS_WITH_ERRORS.failure should be (false)
        Status.FAILED.failure should be (true)
        Status.ABORTED.failure should be (true)
        Status.SKIPPED.failure should be (false)
    }

    "The Status" should "parse correctly" in {
        Status.ofString("UNKNOWN") should be (Status.UNKNOWN)
        Status.ofString("unknown") should be (Status.UNKNOWN)
        Status.ofString("RUNNING") should be (Status.RUNNING)
        Status.ofString("SUCCESS") should be (Status.SUCCESS)
        Status.ofString("SUCCESS_WITH_ERRORS") should be (Status.SUCCESS_WITH_ERRORS)
        Status.ofString("FAILED") should be (Status.FAILED)
        Status.ofString("aborted") should be (Status.ABORTED)
        Status.ofString("killed") should be (Status.ABORTED)
        Status.ofString("skipped") should be (Status.SKIPPED)
        a[NullPointerException] shouldBe thrownBy(Status.ofString(null))
        an[IllegalArgumentException] shouldBe thrownBy(Status.ofString("NO_SUCH_MODE"))
    }

    it should "provide a toString method" in {
        Status.UNKNOWN.toString should be ("UNKNOWN")
        Status.RUNNING.toString should be ("RUNNING")
        Status.SUCCESS.toString should be ("SUCCESS")
        Status.SUCCESS_WITH_ERRORS.toString should be ("SUCCESS_WITH_ERRORS")
        Status.FAILED.toString should be ("FAILED")
        Status.ABORTED.toString should be ("ABORTED")
        Status.SKIPPED.toString should be ("SKIPPED")
    }

    it should "parse toString correctly" in {
        Status.ofString(Status.UNKNOWN.toString) should be (Status.UNKNOWN)
        Status.ofString(Status.RUNNING.toString) should be (Status.RUNNING)
        Status.ofString(Status.SUCCESS.toString) should be (Status.SUCCESS)
        Status.ofString(Status.SUCCESS_WITH_ERRORS.toString) should be (Status.SUCCESS_WITH_ERRORS)
        Status.ofString(Status.FAILED.toString) should be (Status.FAILED)
        Status.ofString(Status.ABORTED.toString) should be (Status.ABORTED)
        Status.ofString(Status.SKIPPED.toString) should be (Status.SKIPPED)
    }
}
