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


class MigrationTest extends AnyFlatSpec with Matchers {
    "The MigrationStrategy" should "parse correctly" in {
        MigrationStrategy.ofString("NEVER") should be (MigrationStrategy.NEVER)
        MigrationStrategy.ofString("never") should be (MigrationStrategy.NEVER)
        MigrationStrategy.ofString("FAIL") should be (MigrationStrategy.FAIL)
        MigrationStrategy.ofString("ALTER") should be (MigrationStrategy.ALTER)
        MigrationStrategy.ofString("ALTER_REPLACE") should be (MigrationStrategy.ALTER_REPLACE)
        MigrationStrategy.ofString("REPLACE") should be (MigrationStrategy.REPLACE)
        a[NullPointerException] shouldBe thrownBy(MigrationStrategy.ofString(null))
        an[IllegalArgumentException] shouldBe thrownBy(MigrationStrategy.ofString("NO_SUCH_MODE"))
    }

    it should "provide a toString method" in {
        MigrationStrategy.NEVER.toString should be ("NEVER")
        MigrationStrategy.FAIL.toString should be ("FAIL")
        MigrationStrategy.ALTER.toString should be ("ALTER")
        MigrationStrategy.ALTER_REPLACE.toString should be ("ALTER_REPLACE")
        MigrationStrategy.REPLACE.toString should be ("REPLACE")
    }

    it should "parse toString correctly" in {
        MigrationStrategy.ofString(MigrationStrategy.NEVER.toString) should be (MigrationStrategy.NEVER)
        MigrationStrategy.ofString(MigrationStrategy.FAIL.toString) should be (MigrationStrategy.FAIL)
        MigrationStrategy.ofString(MigrationStrategy.ALTER.toString) should be (MigrationStrategy.ALTER)
        MigrationStrategy.ofString(MigrationStrategy.ALTER_REPLACE.toString) should be (MigrationStrategy.ALTER_REPLACE)
        MigrationStrategy.ofString(MigrationStrategy.REPLACE.toString) should be (MigrationStrategy.REPLACE)
    }

    "The MigrationPolicy" should "parse correctly" in {
        MigrationPolicy.ofString("RELAXED") should be (MigrationPolicy.RELAXED)
        MigrationPolicy.ofString("relaxed") should be (MigrationPolicy.RELAXED)
        MigrationPolicy.ofString("STRICT") should be (MigrationPolicy.STRICT)
        a[NullPointerException] shouldBe thrownBy(MigrationPolicy.ofString(null))
        an[IllegalArgumentException] shouldBe thrownBy(MigrationPolicy.ofString("NO_SUCH_MODE"))
    }

    it should "provide a toString method" in {
        MigrationPolicy.RELAXED.toString should be ("RELAXED")
        MigrationPolicy.STRICT.toString should be ("STRICT")
    }

    it should "parse toString correctly" in {
        MigrationPolicy.ofString(MigrationPolicy.RELAXED.toString) should be (MigrationPolicy.RELAXED)
        MigrationPolicy.ofString(MigrationPolicy.STRICT.toString) should be (MigrationPolicy.STRICT)
    }
}
