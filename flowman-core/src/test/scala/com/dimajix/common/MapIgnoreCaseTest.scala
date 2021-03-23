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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class MapIgnoreCaseTest extends AnyFlatSpec with Matchers {
    "The MapIgnoreCase" should "work" in {
        val map = MapIgnoreCase(
            "a" -> 1,
            "B" -> 2,
            "X" -> 3
        )

        map.contains("a") should be (true)
        map.contains("A") should be (true)
        map.contains("b") should be (true)
        map.contains("B") should be (true)
        map.contains("c") should be (false)
        map.contains("C") should be (false)

        map.keys should be (Set("a", "b", "x"))
    }

    it should "be constructable from a single element" in {
        val map = MapIgnoreCase(
            "a" -> 1
        )

        map should be (MapIgnoreCase("a" -> 1))
    }

    it should "be constructable from a traditional Map" in {
        val map = MapIgnoreCase(Map(
            "a" -> 1,
            "B" -> 2
        ))

        map should be (MapIgnoreCase("a" -> 1,"B" -> 2))
    }

    it should "be constructable from a traditional Seq" in {
        val map = MapIgnoreCase(Seq(
            "a" -> 1,
            "B" -> 2
        ))

        map should be (MapIgnoreCase("a" -> 1,"B" -> 2))
        map should be (MapIgnoreCase("A" -> 1,"b" -> 2))
    }

    it should "support adding elements" in {
        MapIgnoreCase("a" -> 1) + ("b" -> 2) should be (MapIgnoreCase("a" -> 1,"B" -> 2))
        MapIgnoreCase("a" -> 1) + ("a" -> 2) should be (MapIgnoreCase("a" -> 2))
        MapIgnoreCase("a" -> 1) + ("A" -> 2) should be (MapIgnoreCase("a" -> 2))
    }

    it should "support removing elements" in {
        MapIgnoreCase("a" -> 1) - "b" should be (MapIgnoreCase[Int]("a" -> 1))
        MapIgnoreCase("a" -> 1) - "a" should be (MapIgnoreCase[Int]())
        MapIgnoreCase("a" -> 1) - "A" should be (MapIgnoreCase[Int]())
    }

    it should "support converting to Seq" in {
        val map = MapIgnoreCase[Int](Seq(
            "a" -> 1,
            "B" -> 2
        ))

        map.toSeq should be (Seq("a" -> 1, "B" -> 2))
    }

    it should "support converting to Map" in {
        val map = MapIgnoreCase[Int](Map(
            "a" -> 1,
            "B" -> 2
        ))

        map.toMap should be (Map("a" -> 1, "B" -> 2))
    }

    it should "support mapValues" in {
        val map = MapIgnoreCase[Int](Seq(
            "a" -> 1,
            "B" -> 2
        ))

        map.mapValues(v => 2*v) should be (MapIgnoreCase("a" -> 2, "B" -> 4))
        map.mapValues(v => 2*v).toSeq should be (Seq("a" -> 2, "B" -> 4))
    }
}
