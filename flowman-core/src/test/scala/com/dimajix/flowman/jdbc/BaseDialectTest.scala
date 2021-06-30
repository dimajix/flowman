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

package com.dimajix.flowman.jdbc

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.catalog.PartitionSpec

class BaseDialectTest extends AnyFlatSpec with Matchers {
    "The BaseDialect" should "create PARTITION spects" in {
        val dialect = NoopDialect
        val partitionSpec = PartitionSpec(Map(
            "p1" -> "lala",
            "p2" -> 12
        ))
        dialect.expr.partition(partitionSpec) should be ("""PARTITION(p1='lala',p2=12)""")
    }

    it should "provide appropriate IN expression" in {
        val dialect = NoopDialect
        dialect.expr.in("col", Seq()) should be (""""col" IN ()""")
        dialect.expr.in("col", Seq(1)) should be (""""col" IN (1)""")
        dialect.expr.in("col", Seq(1,7)) should be (""""col" IN (1,7)""")
        dialect.expr.in("col", Seq("left","right")) should be (""""col" IN ('left','right')""")
    }
}
