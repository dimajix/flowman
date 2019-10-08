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

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class StatusTest extends FlatSpec with Matchers {
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
}
