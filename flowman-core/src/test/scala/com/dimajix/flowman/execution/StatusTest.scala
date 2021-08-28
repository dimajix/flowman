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
        Status.ofAll(Seq())(identity) should be (Status.SUCCESS)
    }
    it should "return SUCCESS if all succeed" in {
        Status.ofAll(Seq(Status.SUCCESS, Status.SUCCESS))(identity) should be (Status.SUCCESS)
    }
    it should "return FAILED for any failure" in {
        Status.ofAll(Seq(Status.SKIPPED, Status.FAILED, Status.SUCCESS))(identity) should be (Status.FAILED)
    }
    it should "return FAILED for any UNKNOWN" in {
        Status.ofAll(Seq(Status.SKIPPED, Status.UNKNOWN, Status.SUCCESS))(identity) should be (Status.FAILED)
    }
    it should "return SKIPPED if all are skipped" in {
        Status.ofAll(Seq(Status.SKIPPED, Status.SKIPPED, Status.SUCCESS))(identity) should be (Status.SUCCESS)
        Status.ofAll(Seq(Status.SKIPPED, Status.SKIPPED))(identity) should be (Status.SKIPPED)
    }

    "The Status" should "parse correctly" in {
        Status.ofString("UNKNOWN") should be (Status.UNKNOWN)
        Status.ofString("unknown") should be (Status.UNKNOWN)
        Status.ofString("RUNNING") should be (Status.RUNNING)
        Status.ofString("SUCCESS") should be (Status.SUCCESS)
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
        Status.FAILED.toString should be ("FAILED")
        Status.ABORTED.toString should be ("ABORTED")
        Status.SKIPPED.toString should be ("SKIPPED")
    }

    it should "parse toString correctly" in {
        Status.ofString(Status.UNKNOWN.toString) should be (Status.UNKNOWN)
        Status.ofString(Status.RUNNING.toString) should be (Status.RUNNING)
        Status.ofString(Status.SUCCESS.toString) should be (Status.SUCCESS)
        Status.ofString(Status.FAILED.toString) should be (Status.FAILED)
        Status.ofString(Status.ABORTED.toString) should be (Status.ABORTED)
        Status.ofString(Status.SKIPPED.toString) should be (Status.SKIPPED)
    }
}
