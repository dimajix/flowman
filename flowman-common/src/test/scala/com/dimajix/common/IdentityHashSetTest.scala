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

import com.dimajix.common.IdentityHashSetTest.SomeClass


object IdentityHashSetTest {
    case class SomeClass(value:Int)
}

class IdentityHashSetTest extends AnyFlatSpec with Matchers {
    "The IdentityHashSet" should "work" in {
        val set = IdentityHashSet[SomeClass]()

        val key = SomeClass(3)
        key should be (SomeClass(3))

        set.add(key)
        set.contains(key) should be (true)
        set.contains(SomeClass(3)) should be (false)
    }

    it should "provide empty sets" in {
        val map = IdentityHashSet[SomeClass]()

        map.empty should be (IdentityHashSet.empty)
    }

    it should "preserve identity semantics when copied" in {
        val first = SomeClass(3)
        val second = SomeClass(3)
        val set = IdentityHashSet[SomeClass]()
        set.addOne(first)
        set.addOne(second)

        val cloned = set.clone()
        cloned.size should be (2)
        cloned.contains(first) should be (true)
        cloned.contains(second) should be (true)
        cloned.contains(SomeClass(3)) should be (false)

        val included = set.incl(SomeClass(4))
        included.size should be (3)
        included.contains(SomeClass(3)) should be (false)
        set.size should be (2)

        val excluded = set.excl(first)
        excluded.size should be (1)
        excluded.contains(first) should be (false)
        excluded.contains(second) should be (true)

        val removed = IdentityHashSet[SomeClass]()
        removed.addOne(first)
        val difference = set.diff(removed)
        difference.size should be (1)
        difference.contains(first) should be (false)
        difference.contains(second) should be (true)
    }
}
