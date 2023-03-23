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

package com.dimajix.common

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ExceptionUtilsTest extends AnyFlatSpec with Matchers {
    "ExceptionUtils.reasons" should "work" in {
        val ex1 = new RuntimeException("This is a test")
        ExceptionUtils.reasons(ex1) should be ("java.lang.RuntimeException: This is a test")

        val ex2 = new RuntimeException("This is a test", new RuntimeException("second thing"))
        ExceptionUtils.reasons(ex2) should be ("java.lang.RuntimeException: This is a test\n    caused by: java.lang.RuntimeException: second thing")
    }
}
