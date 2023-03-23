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

package com.dimajix.common.text

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class StringUtilsTest extends AnyFlatSpec with Matchers {
    "StringUtils.truncate" should "work" in {
        StringUtils.truncate(null, 10) should be (null)
        StringUtils.truncate("1234", 4) should be ("1234")
        StringUtils.truncate("1234", 5) should be ("1234")
        StringUtils.truncate("12345", 4) should be ("1...")
        StringUtils.truncate("12345", 5) should be ("12345")
    }
}
