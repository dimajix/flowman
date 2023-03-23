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

package com.dimajix.flowman.catalog

import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.util.UtcTimestamp


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

    it should "create appropriate predicates" in {
        val partitionSpec = PartitionSpec(Map(
            "p_str" -> "lala",
            "p_int" -> 123,
            "p_date" -> java.sql.Date.valueOf("2021-08-03"),
            "p_timestamp" -> UtcTimestamp.parse("2021-08-03T02:03:44")
        ))

        partitionSpec.predicate should be ("p_str='lala' AND p_int=123 AND p_date=date('2021-08-03') AND p_timestamp=timestamp(1627956224)")
    }
}
