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


class TrileanTest extends AnyFlatSpec with Matchers {
    "Trileans" should "provide toString" in {
        No.toString should be ("no")
        Yes.toString should be ("yes")
        Unknown.toString should be ("unknown")
    }

    they should "be castable from booleans" in {
        val no:Trilean = false
        val yes:Trilean = true
        no should be(No)
        yes should be(Yes)
    }

    they should "be commutative" in {
        (Yes || No) should be (No || Yes)
        (Yes && No) should be (No && Yes)
        (Yes || Unknown) should be (Unknown || Yes)
        (Yes && Unknown) should be (Unknown && Yes)
        (Unknown || No) should be (No || Unknown)
        (Unknown && No) should be (No && Unknown)
    }

    they should "provide consistent not" in {
        (!Yes) should be (No)
        (!No) should be (Yes)
        (!Unknown) should be (Unknown)

        (!(Yes || No)) should be (!Yes && !No)
        (!(Yes && No)) should be (!Yes || !No)
        (!(Unknown || No)) should be (!Unknown && !No)
        (!(Unknown && No)) should be (!Unknown || !No)
        (!(Yes || Unknown)) should be (!Yes && !Unknown)
        (!(Yes && Unknown)) should be (!Yes || !Unknown)
    }
}
