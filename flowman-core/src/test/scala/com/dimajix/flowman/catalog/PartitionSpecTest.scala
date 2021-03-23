/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.catalog

import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class PartitionSpecTest extends AnyFlatSpec with Matchers {
    "The PartitionSpec" should "provide a Hive compatible path" in {
        val partitionSpec = PartitionSpec(Map(
            "p1" -> "lala",
            "p2" -> 123
        ))

        val partitions = Seq("p1", "p2")
        partitionSpec.path(new Path("/lala"), partitions) should be (new Path("/lala/p1=lala/p2=123"))
    }

    it should "be case insensitive" in {
        val partitionSpec = PartitionSpec(Map(
            "P1" -> "lala",
            "p2" -> 123
        ))

        val partitions = Seq("p1", "P2")
        partitionSpec.path(new Path("/lala"), partitions) should be (new Path("/lala/P1=lala/p2=123"))
    }
}
