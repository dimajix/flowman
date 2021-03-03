/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.spark.accumulator

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class CounterAccumulatorTest extends AnyFlatSpec with Matchers {
    "The CounterAccumulator" should "accumulate individual values" in {
        val accumulator = new CounterAccumulator()
        accumulator.get("a") should be (None)

        accumulator.add("a")
        accumulator.get("a") should be (Some(1l))
        accumulator.value should be (Map("a" -> 1l))

        accumulator.add("a")
        accumulator.get("a") should be (Some(2l))
        accumulator.value should be (Map("a" -> 2l))

        accumulator.add("b")
        accumulator.get("a") should be (Some(2l))
        accumulator.get("b") should be (Some(1l))
        accumulator.value should be (Map("a" -> 2l, "b" -> 1l))
    }

    it should "accumulate whole maps" in {
        val accumulator = new CounterAccumulator()
        accumulator.add(Map("a" -> 1l, "b" -> 2l))
        accumulator.get("a") should be (Some(1l))
        accumulator.get("b") should be (Some(2l))
        accumulator.value should be (Map("a" -> 1l, "b" -> 2l))

        accumulator.add(Map("a" -> 1l, "c" -> 3l))
        accumulator.get("a") should be (Some(2l))
        accumulator.get("b") should be (Some(2l))
        accumulator.get("c") should be (Some(3l))
        accumulator.value should be (Map("a" -> 2l, "b" -> 2l, "c" -> 3l))
    }

    it should "support merging two accumulators" in {
        val accumulator = new CounterAccumulator()
        accumulator.add(Map("a" -> 1l, "b" -> 2l))

        val other = new CounterAccumulator()
        other.add(Map("b" -> 1l, "c" -> 7l))

        accumulator.merge(other)
        accumulator.value should be (Map("a" -> 1l, "b" -> 3l, "c" -> 7l))
    }

    it should "support copying" in {
        val accumulator = new CounterAccumulator()
        accumulator.add(Map("a" -> 1l, "b" -> 2l))

        val copy = accumulator.copy
        copy.add(Map("b" -> 1l, "c" -> 7l))

        accumulator.value should be (Map("a" -> 1l, "b" -> 2l))
        copy.value should be (Map("a" -> 1l, "b" -> 3l, "c" -> 7l))
    }
}
