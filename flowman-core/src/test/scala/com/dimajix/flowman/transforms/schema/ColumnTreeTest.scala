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

package com.dimajix.flowman.transforms.schema

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ColumnTreeTest extends AnyFlatSpec with Matchers {
    import com.dimajix.flowman.transforms.schema.ColumnTree.implicits._

    "The ColumnTree" should "create the same schema via round-trip" in {
        val inputSchema = StructType(Seq(
            StructField("col1", StringType, false),
            StructField("COL2", StructType(
                Seq(
                    StructField("nested1", StringType, false),
                    StructField("nested3", ArrayType(FloatType), false),
                    StructField("nested4", StructType(
                        Seq(
                            StructField("nested4_1", StringType, false),
                            StructField("nested4_2", FloatType, false)
                        )
                    ), false)
                )
            ), false),
            StructField("col3", IntegerType, false)
        ))
        val root = ColumnTree.ofSchema(inputSchema)
        root.nullable should be (false)

        val columns = root.mkValue()
        val expected = struct(
            col("col1"),
            col("COL2"),
            col("col3")
        )

        // This doesn't work:
        //      columns should be (expected)
        // therefore we compare string representation
        columns.toString() should be (expected.toString())
    }

    it should "support dropping non existing paths" in {
        val inputSchema = StructType(Seq(
            StructField("col1", StringType, false),
            StructField("COL2", StructType(
                Seq(
                    StructField("nested1", StringType, false),
                    StructField("nested3", FloatType, false),
                    StructField("nested4", StructType(
                        Seq(
                            StructField("nested4_1", StringType, false),
                            StructField("nested4_2", FloatType, false)
                        )
                    ), false)
                )
            ), false),
            StructField("col3", IntegerType, false)
        ))
        val root = ColumnTree.ofSchema(inputSchema)
            .drop(Path("no_such_column"))
        val columns = root.mkValue()

        val expected = struct(
            col("col1"),
            col("COL2"),
            col("col3")
        )

        // This doesn't work:
        //      columns should be (expected)
        // therefore we compare string representation
        columns.toString() should be (expected.toString())
    }

    it should "support dropping existing paths" in {
        val inputSchema = StructType(Seq(
            StructField("col1", StringType, false),
            StructField("COL2", StructType(
                Seq(
                    StructField("nested1", StringType, false),
                    StructField("nested3", FloatType, false),
                    StructField("nested4", StructType(
                        Seq(
                            StructField("nested4_1", StringType, false),
                            StructField("nested4_2", FloatType, false)
                        )
                    ), false)
                )
            ), false),
            StructField("col3", IntegerType, false)
        ))
        val root = ColumnTree.ofSchema(inputSchema)
            .drop(Path("col1"))
        val columns = root.mkValue()

        val expected = struct(
            col("COL2"),
            col("col3")
        )

        // This doesn't work:
        //      columns should be (expected)
        // therefore we compare string representation
        columns.toString() should be (expected.toString())
    }

    it should "support dropping nested paths" in {
        val inputSchema = StructType(Seq(
            StructField("col1", StringType, false),
            StructField("COL2", StructType(
                Seq(
                    StructField("nested1", StringType, false),
                    StructField("nested3", FloatType, false),
                    StructField("nested4", StructType(
                        Seq(
                            StructField("nested4_1", StringType, false),
                            StructField("nested4_2", FloatType, false)
                        )
                    ), false)
                )
            ), false)
        ))
        val root = ColumnTree.ofSchema(inputSchema)
            .drop(Path("COL2.nested4"))
        val columns = root.mkValue()

        val expected = struct(
            col("col1"),
            struct(
                col("COL2.nested1") as "nested1",
                col("COL2.nested3") as "nested3"
            ).as("COL2")
        )

        // This doesn't work:
        //      columns should be (expected)
        // therefore we compare string representation
        columns.toString() should be (expected.toString())
    }

    it should "support dropping everything" in {
        val inputSchema = StructType(Seq(
            StructField("col1", StringType, false),
            StructField("COL2", StructType(
                Seq(
                    StructField("nested1", StringType, false),
                    StructField("nested3", FloatType, false),
                    StructField("nested4", StructType(
                        Seq(
                            StructField("nested4_1", StringType, false),
                            StructField("nested4_2", FloatType, false)
                        )
                    ), false)
                )
            ), false)
        ))
        val root = ColumnTree.ofSchema(inputSchema)
            .drop(Path("COL2.*"))
        val columns = root.mkValue()

        val expected = struct(
            col("col1")
        )

        // This doesn't work:
        //      columns should be (expected)
        // therefore we compare string representation
        columns.toString() should be (expected.toString())
    }

    it should "remove empty structures" in {
        val inputSchema = StructType(Seq(
            StructField("col1", StringType, false),
            StructField("COL2", StructType(
                Seq(
                    StructField("nested1", StringType, false),
                    StructField("nested3", FloatType, false),
                    StructField("nested4", StructType(
                        Seq(
                            StructField("nested4_1", StringType, false),
                            StructField("nested4_2", FloatType, false)
                        )
                    ), false)
                )
            ), false)
        ))
        val root = ColumnTree.ofSchema(inputSchema)
            .drop(Path("COL2.nested4.nested4_1"))
            .drop(Path("COL2.nested4.nested4_2"))
            .drop(Path("COL2.nested1"))
            .drop(Path("COL2.nested3"))
        val columns = root.mkValue()

        val expected = struct(
            col("col1")
        )

        // This doesn't work:
        //      columns should be (expected)
        // therefore we compare string representation
        columns.toString() should be (expected.toString())
    }

    it should "support dropping non existing nested paths" in {
        val inputSchema = StructType(Seq(
            StructField("col1", StringType, false),
            StructField("COL2", StructType(
                Seq(
                    StructField("nested1", StringType, false),
                    StructField("nested3", FloatType, false)
                )
            ), false),
            StructField("col3", IntegerType, false)
        ))
        val root = ColumnTree.ofSchema(inputSchema)
            .drop(Path("COL2.nested4"))
        val columns = root.mkValue()

        val expected = struct(
            col("col1"),
            col("COL2"),
            col("col3")
        )

        // This doesn't work:
        //      columns should be (expected)
        // therefore we compare string representation
        columns.toString() should be (expected.toString())
    }

    it should "support explode on simple arrays via NodeOps" in {
        val inputSchema = StructType(Seq(
            StructField("COL2", StructType(
                Seq(
                    StructField("nested", ArrayType(StringType))
                )
            ))
        ))
        val root = ColumnTree.ofSchema(inputSchema)
        val child = root.find(Path("COL2.nested"))

        val columns = columnNodeOps.explode("exploded", child.get.mkValue())

        val expected = explode(col("COL2.nested") as "nested") as "exploded"

        // This doesn't work:
        //      columns should be (expected)
        // therefore we compare string representation
        columns.toString() should be (expected.toString())
    }

    it should "support explode on structured arrays via NodeOps" in {
        val inputSchema = StructType(Seq(
            StructField("COL2", StructType(
                Seq(
                    StructField("nested", ArrayType(StructType(Seq(
                        StructField("int", IntegerType),
                        StructField("str", StringType)
                    ))))
                )
            ))
        ))
        val root = ColumnTree.ofSchema(inputSchema)
        val child = root.find(Path("COL2.nested"))

        val columns = columnNodeOps.explode("exploded", child.get.mkValue())

        val expected = explode(col("COL2.nested") as "nested") as "exploded"

        // This doesn't work:
        //      columns should be (expected)
        // therefore we compare string representation
        columns.toString() should be (expected.toString())
    }
}
