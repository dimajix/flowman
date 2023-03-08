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

package com.dimajix.flowman.common

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ParserUtilsTest extends AnyFlatSpec with Matchers {
    "ParserUtils.splitSetting" should "work" in {
        ParserUtils.splitSetting("a=b") should be ("a" -> "b")
        ParserUtils.splitSetting("a=") should be ("a" -> "")
        ParserUtils.splitSetting("a=b=c") should be ("a" -> "b=c")
        ParserUtils.splitSetting("""a="b=c"""") should be ("a" -> "b=c")
        ParserUtils.splitSetting("""a=""""") should be ("a" -> "")
    }

    "ParserUtils.parseDelimitedList" should "work" in {
        ParserUtils.parseDelimitedList("a, b, ,cde") should be (Seq("a","b","cde"))
    }

    "ParserUtils.parseDelimitedKeyValues" should "work" in {
        ParserUtils.parseDelimitedKeyValues("a=x, b=yz=abc, ,cde") should be (Map("a" -> "x", "b" -> "yz=abc"))
        ParserUtils.parseDelimitedKeyValues("a=x, b=, ,cde") should be (Map("a" -> "x", "b" -> ""))
        ParserUtils.parseDelimitedKeyValues("a=x, b=\"yz=abc\", ,cde") should be (Map("a" -> "x", "b" -> "yz=abc"))
        ParserUtils.parseDelimitedKeyValues("a=x, b=,\"\" ,cde") should be (Map("a" -> "x", "b" -> ""))
    }
}
