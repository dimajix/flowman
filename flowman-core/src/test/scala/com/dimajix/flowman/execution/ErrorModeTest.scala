/*
 * Copyright (C) 2021 The Flowman Authors
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


class ErrorModeTest extends AnyFlatSpec with Matchers {
    "The ErrorMode" should "parse correctly" in {
        ErrorMode.ofString("FAIL_FAST") should be (ErrorMode.FAIL_FAST)
        ErrorMode.ofString("fail_fast") should be (ErrorMode.FAIL_FAST)
        ErrorMode.ofString("failfast") should be (ErrorMode.FAIL_FAST)
        ErrorMode.ofString("failnever") should be (ErrorMode.FAIL_NEVER)
        ErrorMode.ofString("fail_never") should be (ErrorMode.FAIL_NEVER)
        ErrorMode.ofString("failatend") should be (ErrorMode.FAIL_AT_END)
        ErrorMode.ofString("fail_at_end") should be (ErrorMode.FAIL_AT_END)
        a[NullPointerException] shouldBe thrownBy(ErrorMode.ofString(null))
        an[IllegalArgumentException] shouldBe thrownBy(ErrorMode.ofString("NO_SUCH_MODE"))
    }

    it should "provide a toString method" in {
        ErrorMode.FAIL_FAST.toString should be ("FAIL_FAST")
        ErrorMode.FAIL_NEVER.toString should be ("FAIL_NEVER")
        ErrorMode.FAIL_AT_END.toString should be ("FAIL_AT_END")
    }

    it should "parse toString correctly" in {
        ErrorMode.ofString(ErrorMode.FAIL_FAST.toString) should be (ErrorMode.FAIL_FAST)
        ErrorMode.ofString(ErrorMode.FAIL_NEVER.toString) should be (ErrorMode.FAIL_NEVER)
        ErrorMode.ofString(ErrorMode.FAIL_AT_END.toString) should be (ErrorMode.FAIL_AT_END)
    }
}
