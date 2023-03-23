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

package com.dimajix.flowman.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class BuildPolicyTest extends AnyFlatSpec with Matchers {
    "The BuildPolicy" should "parse correctly" in {
        BuildPolicy.ofString("ALWAYS") should be(BuildPolicy.ALWAYS)
        BuildPolicy.ofString("always") should be(BuildPolicy.ALWAYS)
        BuildPolicy.ofString("IF_EMPTY") should be(BuildPolicy.IF_EMPTY)
        BuildPolicy.ofString("ifempty") should be(BuildPolicy.IF_EMPTY)
        BuildPolicy.ofString("if_empty") should be(BuildPolicy.IF_EMPTY)
        BuildPolicy.ofString("IF_TAINTED") should be(BuildPolicy.IF_TAINTED)
        BuildPolicy.ofString("IFTAINTED") should be(BuildPolicy.IF_TAINTED)
        BuildPolicy.ofString("if_tainted") should be(BuildPolicy.IF_TAINTED)
        BuildPolicy.ofString("SMART") should be(BuildPolicy.SMART)
        BuildPolicy.ofString("COMPAT") should be(BuildPolicy.COMPAT)
        a[NullPointerException] shouldBe thrownBy(BuildPolicy.ofString(null))
        an[IllegalArgumentException] shouldBe thrownBy(BuildPolicy.ofString("NO_SUCH_POLICY"))
    }

    it should "provide a toString method" in {
        BuildPolicy.ALWAYS.toString should be("ALWAYS")
        BuildPolicy.IF_EMPTY.toString should be("IF_EMPTY")
        BuildPolicy.IF_TAINTED.toString should be("IF_TAINTED")
        BuildPolicy.SMART.toString should be("SMART")
        BuildPolicy.COMPAT.toString should be("COMPAT")
    }

    it should "parse toString correctly" in {
        BuildPolicy.ofString(BuildPolicy.ALWAYS.toString) should be(BuildPolicy.ALWAYS)
        BuildPolicy.ofString(BuildPolicy.IF_EMPTY.toString) should be(BuildPolicy.IF_EMPTY)
        BuildPolicy.ofString(BuildPolicy.IF_TAINTED.toString) should be(BuildPolicy.IF_TAINTED)
        BuildPolicy.ofString(BuildPolicy.SMART.toString) should be(BuildPolicy.SMART)
        BuildPolicy.ofString(BuildPolicy.COMPAT.toString) should be(BuildPolicy.COMPAT)
    }
}
