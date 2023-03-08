/*
 * Copyright (C) 2018 The Flowman Authors
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


class OutputModeTest extends AnyFlatSpec with Matchers {
    "The OutputMode" should "parse correctly" in {
        OutputMode.ofString("OVERWRITE") should be (OutputMode.OVERWRITE)
        OutputMode.ofString("overwrite") should be (OutputMode.OVERWRITE)
        OutputMode.ofString("APPEND") should be (OutputMode.APPEND)
        OutputMode.ofString("UPDATE") should be (OutputMode.UPDATE)
        OutputMode.ofString("UPSERT") should be (OutputMode.UPDATE)
        OutputMode.ofString("IGNORE_IF_EXISTS") should be (OutputMode.IGNORE_IF_EXISTS)
        OutputMode.ofString("ERROR_IF_EXISTS") should be (OutputMode.ERROR_IF_EXISTS)
        a[NullPointerException] shouldBe thrownBy(OutputMode.ofString(null))
        an[IllegalArgumentException] shouldBe thrownBy(OutputMode.ofString("NO_SUCH_MODE"))
    }

    it should "provide a toString method" in {
        OutputMode.OVERWRITE.toString should be ("OVERWRITE")
        OutputMode.APPEND.toString should be ("APPEND")
        OutputMode.UPDATE.toString should be ("UPDATE")
        OutputMode.IGNORE_IF_EXISTS.toString should be ("IGNORE_IF_EXISTS")
        OutputMode.ERROR_IF_EXISTS.toString should be ("ERROR_IF_EXISTS")
    }

    it should "parse toString correctly" in {
        OutputMode.ofString(OutputMode.OVERWRITE.toString) should be (OutputMode.OVERWRITE)
        OutputMode.ofString(OutputMode.APPEND.toString) should be (OutputMode.APPEND)
        OutputMode.ofString(OutputMode.UPDATE.toString) should be (OutputMode.UPDATE)
        OutputMode.ofString(OutputMode.IGNORE_IF_EXISTS.toString) should be (OutputMode.IGNORE_IF_EXISTS)
        OutputMode.ofString(OutputMode.ERROR_IF_EXISTS.toString) should be (OutputMode.ERROR_IF_EXISTS)
    }
}
