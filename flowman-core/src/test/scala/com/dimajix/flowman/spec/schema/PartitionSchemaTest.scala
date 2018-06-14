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

package com.dimajix.flowman.spec.schema

import org.apache.hadoop.fs.Path
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session


class PartitionSchemaTest extends FlatSpec with Matchers {
    "The PartitionSchema" should "provide partition column names" in {
        val partitionColumns = Seq(
            PartitionField("p1", StringType),
            PartitionField("p2", StringType)
        )
        val partitionSchema = PartitionSchema(partitionColumns)

        partitionSchema.names should be (Seq("p1", "p2"))
    }

    it should "provide a Hive partition spec" in {
        val partitionColumns = Seq(
            PartitionField("p1", StringType),
            PartitionField("p2", StringType)
        )
        val partitionSchema = PartitionSchema(partitionColumns)

        val session = Session.builder().build()
        implicit val context = session.context
        val partitions = Map(
            "p1" -> SingleValue("lala"),
            "p2" -> SingleValue("lolo")
        )
        partitionSchema.partitionSpec(partitions) should be ("PARTITION(p1='lala',p2='lolo')")
    }

    it should "provide a Hive compatible path" in {
        val partitionColumns = Seq(
            PartitionField("p1", StringType),
            PartitionField("p2", StringType)
        )
        val partitionSchema = PartitionSchema(partitionColumns)

        val session = Session.builder().build()
        implicit val context = session.context
        val partitions = Map(
            "p1" -> SingleValue("lala"),
            "p2" -> SingleValue("lolo")
        )
        partitionSchema.partitionPath(new Path("/lala"), partitions) should be (new Path("/lala/p1=lala/p2=lolo"))
    }
}
