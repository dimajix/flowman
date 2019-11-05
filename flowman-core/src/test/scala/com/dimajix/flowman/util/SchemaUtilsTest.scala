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

package com.dimajix.flowman.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.CharType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.VarcharType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.spark.testing.LocalSparkSession
import com.dimajix.spark.testing.QueryTest


class SchemaUtilsTest extends FlatSpec with Matchers with LocalSparkSession with QueryTest {
    "SchemaUtils.toLowerCase" should "convert all names to lower case" in {
        val schema = StructType(
            StructField("Name", StringType) ::
            StructField("Nested", StructType(
                StructField("AmOUnt", DoubleType) ::
                StructField("SomeArray", ArrayType(IntegerType)) ::
                Nil
            )) ::
                StructField("StructArray", ArrayType(
                    StructType(
                        StructField("Name", StringType) ::
                        Nil
                    )
                )
            ) ::
            Nil
        )

        val expected = StructType(
            StructField("name", StringType) ::
                StructField("nested", StructType(
                    StructField("amount", DoubleType) ::
                        StructField("somearray", ArrayType(IntegerType)) ::
                        Nil
                )) ::
                StructField("structarray", ArrayType(
                    StructType(
                        StructField("name", StringType) ::
                            Nil
                    )
                )
                ) ::
                Nil
        )

        val result = SchemaUtils.toLowerCase(schema)
        result should be(expected)
    }

    "SchemaUtils.applySchema" should "work" in {
        val desiredSchema = StructType(Seq(
            StructField("str_col", StringType),
            StructField("float_col", FloatType),
            StructField("int_col", IntegerType)
        ))

        val inputSchema = StructType(Seq(
            StructField("int_col", StringType),
            StructField("str_col", LongType)
        ))
        val rows = spark.sparkContext.parallelize(Seq(
            Row("27", 23l)
        ))
        val df = spark.createDataFrame(rows, inputSchema)

        val result = SchemaUtils.applySchema(df, Some(desiredSchema))
        result.schema should be (desiredSchema)
        val expectedRows = Seq(
            Row("23", null, 27)
        )
        checkAnswer(result, expectedRows)
    }

    it should "add NULL values" in {

    }

    "SchemaUtils.dropMetadata" should "remove all meta data" in {
        val comment = "123456789"
        val schema = StructType(Seq(
            StructField("Name", StringType, false).withComment(comment),
            StructField("Nested",
                StructType(Seq(
                    StructField("AmOUnt", DoubleType).withComment(comment),
                    StructField("SomeArray", ArrayType(IntegerType, false)).withComment(comment)
                ))
            ).withComment(comment),
            StructField("StructArray", ArrayType(
                StructType(Seq(
                    StructField("Name", StringType).withComment(comment)
                ))
            )).withComment(comment)
        ))

        val pureSchema = SchemaUtils.dropMetadata(schema)

        val expectedSchema = StructType(Seq(
            StructField("Name", StringType, false),
            StructField("Nested",
                StructType(Seq(
                    StructField("AmOUnt", DoubleType),
                    StructField("SomeArray", ArrayType(IntegerType, false))
                ))
            ),
            StructField("StructArray", ArrayType(
                StructType(Seq(
                    StructField("Name", StringType)
                ))
            ))
        ))
        pureSchema should be (expectedSchema)
    }

    "SchemaUtils.find" should "find nested fields" in {
        val schema = StructType(Seq(
            StructField("Name", StringType),
            StructField("Nested",
                StructType(Seq(
                    StructField("AmOUnt", DoubleType),
                    StructField("SomeArray", ArrayType(IntegerType))
                ))
            ),
            StructField("StructArray", ArrayType(
                StructType(Seq(
                    StructField("Name", StringType)
                ))
            ))
        ))

        SchemaUtils.find(schema, "no_such_field") should be (None)
        SchemaUtils.find(schema, "Name") should be (Some(schema("Name")))
        SchemaUtils.find(schema, "name") should be (Some(schema("Name")))
        SchemaUtils.find(schema, "nested.amount") should be (Some(StructField("AmOUnt", DoubleType)))
        SchemaUtils.find(schema, "StructArray.Name") should be (Some(StructField("Name", StringType)))
    }

    "SchemaUtils.truncateComments" should "work" in {
        val comment = "123456789"
        val schema = StructType(Seq(
            StructField("Name", StringType).withComment(comment),
            StructField("Nested",
                StructType(Seq(
                    StructField("AmOUnt", DoubleType).withComment(comment),
                    StructField("SomeArray", ArrayType(IntegerType)).withComment(comment)
                ))
            ).withComment(comment),
            StructField("StructArray", ArrayType(
                StructType(Seq(
                    StructField("Name", StringType).withComment(comment)
                ))
            )).withComment(comment)
        ))

        val truncatedSchema = SchemaUtils.truncateComments(schema, 3)

        val expectedComment = comment.take(3)
        val expectedSchema = StructType(Seq(
            StructField("Name", StringType).withComment(expectedComment),
            StructField("Nested",
                StructType(Seq(
                    StructField("AmOUnt", DoubleType).withComment(expectedComment),
                    StructField("SomeArray", ArrayType(IntegerType)).withComment(expectedComment)
                ))
            ).withComment(expectedComment),
            StructField("StructArray", ArrayType(
                StructType(Seq(
                    StructField("Name", StringType).withComment(expectedComment)
                ))
            )).withComment(expectedComment)
        ))
        truncatedSchema should be (expectedSchema)
    }

    "SchemaUtils.merge" should "merge two fields" in {
        SchemaUtils.merge(
            StructField("f1", IntegerType, true),
            StructField("f1", LongType, true)
        ) should be (
            StructField("f1", LongType, true)
        )

        SchemaUtils.merge(
            StructField("f1", IntegerType, true),
            StructField("f1", StringType, true)
        ) should be (
            StructField("f1", StringType, true)
        )

        SchemaUtils.merge(
            StructField("f1", IntegerType, true),
            StructField("f1", StringType, true)
        ) should be (
            StructField("f1", StringType, true)
        )
    }

    it should "pay attention to nullability" in {
        SchemaUtils.merge(
            StructField("f1", IntegerType, true),
            StructField("f1", LongType, false)
        ) should be (
            StructField("f1", LongType, true)
        )

        SchemaUtils.merge(
            StructField("f1", IntegerType, false),
            StructField("f1", LongType, true)
        ) should be (
            StructField("f1", LongType, true)
        )

        SchemaUtils.merge(
            StructField("f1", IntegerType, false),
            StructField("f1", LongType, false)
        ) should be (
            StructField("f1", LongType, false)
        )
    }

    it should "pay attention to NullTypes" in {
        SchemaUtils.merge(
            StructField("f1", NullType, false),
            StructField("f1", LongType, false)
        ) should be (
            StructField("f1", LongType, true)
        )

        SchemaUtils.merge(
            StructField("f1", NullType, false),
            StructField("f1", LongType, false)
        ) should be (
            StructField("f1", LongType, true)
        )
    }


    "SchemaUtils.coerce" should "coerce two data types" in {
        SchemaUtils.coerce(IntegerType, IntegerType) should be (IntegerType)
        SchemaUtils.coerce(LongType, IntegerType) should be (LongType)

        SchemaUtils.coerce(StringType, IntegerType) should be (StringType)
        SchemaUtils.coerce(StringType, BooleanType) should be (StringType)
        SchemaUtils.coerce(StringType, FloatType) should be (StringType)
        SchemaUtils.coerce(StringType, DecimalType(20,10)) should be (StringType)

        SchemaUtils.coerce(IntegerType, StringType) should be (StringType)
        SchemaUtils.coerce(BooleanType, StringType) should be (StringType)
        SchemaUtils.coerce(FloatType, StringType) should be (StringType)
        SchemaUtils.coerce(DecimalType(20,10), StringType) should be (StringType)
    }

    it should "coerce null types" in {
        SchemaUtils.coerce(IntegerType, NullType) should be (IntegerType)
        SchemaUtils.coerce(BooleanType, NullType) should be (BooleanType)

        SchemaUtils.coerce(NullType, IntegerType) should be (IntegerType)
        SchemaUtils.coerce(NullType, BooleanType) should be (BooleanType)
    }

    it should "coerce integral and fractional types" in {
        SchemaUtils.coerce(IntegerType, FloatType) should be (DoubleType)
        SchemaUtils.coerce(LongType, FloatType) should be (DoubleType)
        SchemaUtils.coerce(LongType, DoubleType) should be (DoubleType)

        SchemaUtils.coerce(FloatType, IntegerType) should be (DoubleType)
        SchemaUtils.coerce(FloatType, LongType) should be (DoubleType)
        SchemaUtils.coerce(DoubleType, LongType) should be (DoubleType)
    }

    it should "coerce Boolean types" in {
        SchemaUtils.coerce(BooleanType, BooleanType) should be (BooleanType)
        SchemaUtils.coerce(LongType, BooleanType) should be (StringType)
        SchemaUtils.coerce(DoubleType, BooleanType) should be (StringType)

        SchemaUtils.coerce(BooleanType, LongType) should be (StringType)
        SchemaUtils.coerce(BooleanType, DoubleType) should be (StringType)
    }

    it should "coerce Decimal types" in {
        SchemaUtils.coerce(LongType, DecimalType(4,2)) should be (DecimalType(22,2))
        SchemaUtils.coerce(IntegerType, DecimalType(4,2)) should be (DecimalType(12,2))
        SchemaUtils.coerce(DoubleType, DecimalType(4,2)) should be (DoubleType)

        SchemaUtils.coerce(DecimalType(4,2), LongType) should be (DecimalType(22,2))
        SchemaUtils.coerce(DecimalType(4,2), IntegerType) should be (DecimalType(12,2))

        SchemaUtils.coerce(DecimalType(10,1), DecimalType(4,2)) should be (DecimalType(11,2))
        SchemaUtils.coerce(DecimalType(4,2), DecimalType(10,1)) should be (DecimalType(11,2))
    }

    it should "coerce VarChar and Char types" in {
        SchemaUtils.coerce(VarcharType(10), VarcharType(10)) should be (VarcharType(10))
        SchemaUtils.coerce(VarcharType(20), VarcharType(10)) should be (VarcharType(20))
        SchemaUtils.coerce(VarcharType(10), VarcharType(20)) should be (VarcharType(20))

        SchemaUtils.coerce(CharType(10), CharType(10)) should be (CharType(10))
        SchemaUtils.coerce(VarcharType(20), VarcharType(10)) should be (VarcharType(20))
        SchemaUtils.coerce(VarcharType(10), VarcharType(20)) should be (VarcharType(20))
    }
}
