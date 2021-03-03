/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.catalog.PartitionSpec
import com.dimajix.flowman.types.ArrayValue
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.RangeValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StringType


class PartitionSchemaTest extends AnyFlatSpec with Matchers {
    "The PartitionSchema" should "provide partition column names" in {
        val partitionColumns = Seq(
            PartitionField("p1", StringType),
            PartitionField("p2", IntegerType)
        )
        val partitionSchema = PartitionSchema(partitionColumns)

        partitionSchema.names should be (Seq("p1", "p2"))
    }

    it should "create a spec" in {
        val partitionColumns = Seq(
            PartitionField("P1", StringType),
            PartitionField("p2", IntegerType)
        )
        val partitionSchema = PartitionSchema(partitionColumns)

        val partitions = Map(
            "p1" -> SingleValue("lala"),
            "p2" -> SingleValue("123")
        )

        val spec = partitionSchema.spec(partitions)
        spec("p1") should be ("lala")
        spec("P1") should be ("lala")
        spec("p2") should be (123)
        spec("P2") should be (123)
        spec.toMap should be (Map("P1" -> "lala", "p2" -> 123))
    }

    it should "interpolate partition values" in {
        val partitionColumns = Seq(
            PartitionField("p1", StringType),
            PartitionField("p2", IntegerType)
        )
        val partitionSchema = PartitionSchema(partitionColumns)

        val partitions = Map(
            "p1" -> ArrayValue("lala", "lolo"),
            "p2" -> RangeValue("123", "127", Some("2"))
        )

        val all = partitionSchema.interpolate(partitions)
        all.toSeq should be (Seq(
            PartitionSpec(Map("p1" -> "lala", "p2" -> 123)),
            PartitionSpec(Map("p1" -> "lala", "p2" -> 125)),
            PartitionSpec(Map("p1" -> "lolo", "p2" -> 123)),
            PartitionSpec(Map("p1" -> "lolo", "p2" -> 125))
        ))
    }

    it should "be case insensitive" in {
        val partitionColumns = Seq(
            PartitionField("P1", StringType),
            PartitionField("p2", IntegerType)
        )
        val partitionSchema = PartitionSchema(partitionColumns)

        partitionSchema.get("p1").name should be ("P1")
        partitionSchema.get("P1").name should be ("P1")
        partitionSchema.get("p2").name should be ("p2")
        partitionSchema.get("P2").name should be ("p2")

        partitionSchema.names should be (Seq("P1", "p2"))
    }
}
