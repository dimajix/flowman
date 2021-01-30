/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.common.IdentityHashMapTest.SomeClass

object IdentityHashMapTest {
    case class SomeClass(value:Int)
}

class IdentityHashMapTest extends FlatSpec with Matchers {
    "The IdentityHashMap" should "work" in {
        val map = IdentityHashMap[SomeClass,String]()

        val key = SomeClass(3)
        key should be (SomeClass(3))

        map.put(key, "three")
        map.contains(key) should be (true)
        map.contains(SomeClass(3)) should be (false)
    }

    it should "provide empty maps" in {
        val map = IdentityHashMap[SomeClass,String]()

        map.empty should be (IdentityHashMap.empty)
    }
}
