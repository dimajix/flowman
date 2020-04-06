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

package com.dimajix.flowman.types

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class SchemaUtilsTest extends FlatSpec with Matchers {
    "SchemaUtils.merge" should "merge two fields" in {
        SchemaUtils.merge(
            Field("f1", IntegerType, true),
            Field("f1", LongType, true)
        ) should be (
            Field("f1", LongType, true)
        )

        SchemaUtils.merge(
            Field("f1", IntegerType, true),
            Field("f1", StringType, true)
        ) should be (
            Field("f1", StringType, true)
        )

        SchemaUtils.merge(
            Field("f1", IntegerType, true),
            Field("f1", StringType, true)
        ) should be (
            Field("f1", StringType, true)
        )
    }

    it should "pay attention to nullability" in {
        SchemaUtils.merge(
            Field("f1", IntegerType, true),
            Field("f1", LongType, false)
        ) should be (
            Field("f1", LongType, true)
        )

        SchemaUtils.merge(
            Field("f1", IntegerType, false),
            Field("f1", LongType, true)
        ) should be (
            Field("f1", LongType, true)
        )

        SchemaUtils.merge(
            Field("f1", IntegerType, false),
            Field("f1", LongType, false)
        ) should be (
            Field("f1", LongType, false)
        )
    }

    it should "pay attention to NullTypes" in {
        SchemaUtils.merge(
            Field("f1", NullType, false),
            Field("f1", LongType, false)
        ) should be (
            Field("f1", LongType, true)
        )

        SchemaUtils.merge(
            Field("f1", NullType, false),
            Field("f1", LongType, false)
        ) should be (
            Field("f1", LongType, true)
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

    it should "limit DecimlTypes to supported precisions" in {
        SchemaUtils.coerce(DecimalType(38,2), DecimalType(38,10)) should be (DecimalType(38,10))
        SchemaUtils.coerce(DecimalType(38,2), DecimalType(38,10)).sparkType should be (org.apache.spark.sql.types.DecimalType(38,10))
    }

    it should "coerce VarChar and Char types" in {
        SchemaUtils.coerce(VarcharType(10), VarcharType(10)) should be (VarcharType(10))
        SchemaUtils.coerce(VarcharType(20), VarcharType(10)) should be (VarcharType(20))
        SchemaUtils.coerce(VarcharType(10), VarcharType(20)) should be (VarcharType(20))

        SchemaUtils.coerce(CharType(10), CharType(10)) should be (CharType(10))
        SchemaUtils.coerce(VarcharType(20), VarcharType(10)) should be (VarcharType(20))
        SchemaUtils.coerce(VarcharType(10), VarcharType(20)) should be (VarcharType(20))

        SchemaUtils.coerce(CharType(10), DoubleType) should be (StringType)
        SchemaUtils.coerce(VarcharType(20), LongType) should be (StringType)

        SchemaUtils.coerce(DoubleType, CharType(10)) should be (StringType)
        SchemaUtils.coerce(LongType, VarcharType(20)) should be (StringType)
    }

    it should "support arrays" in {
        SchemaUtils.coerce(ArrayType(VarcharType(10)), ArrayType(VarcharType(10))) should be (ArrayType(VarcharType(10)))

        SchemaUtils.coerce(ArrayType(IntegerType, false), ArrayType(DecimalType(4,2), false)) should be (ArrayType(DecimalType(12,2), false))
        SchemaUtils.coerce(ArrayType(IntegerType, true), ArrayType(DecimalType(4,2), false)) should be (ArrayType(DecimalType(12,2), true))
        SchemaUtils.coerce(ArrayType(IntegerType, false), ArrayType(DecimalType(4,2), true)) should be (ArrayType(DecimalType(12,2), true))
    }

    it should "support structs" in {
        SchemaUtils.coerce(
            StructType(Seq(
                Field("common_1", DoubleType),
                Field("common_2", VarcharType(10)),
                Field("left_1", IntegerType),
                Field("left_2", IntegerType, false)
            )),
            StructType(Seq(
                Field("common_1", DoubleType),
                Field("common_2", CharType(20)),
                Field("right_1", IntegerType),
                Field("right_2", IntegerType, false)
            ))
        ) should be (
            StructType(Seq(
                Field("common_1", DoubleType),
                Field("common_2", VarcharType(20)),
                Field("right_1", IntegerType),
                Field("right_2", IntegerType, true),
                Field("left_1", IntegerType),
                Field("left_2", IntegerType, true)
            ))
        )
    }


    "SchemaUtils.union" should "work" in {
        SchemaUtils.union(
            StructType(Seq(
                Field("common_1", DoubleType),
                Field("common_2", VarcharType(10)),
                Field("left_1", IntegerType),
                Field("left_2", IntegerType, false)
            )),
            StructType(Seq(
                Field("common_1", DoubleType),
                Field("common_2", CharType(20)),
                Field("right_1", IntegerType),
                Field("right_2", IntegerType, false)
            ))
        ) should be (
            StructType(Seq(
                Field("common_1", DoubleType),
                Field("common_2", VarcharType(20)),
                Field("right_1", IntegerType),
                Field("right_2", IntegerType, true),
                Field("left_1", IntegerType),
                Field("left_2", IntegerType, true)
            ))
        )
    }

    it should "support arrays" in {
        SchemaUtils.union(
            StructType(Seq(
                Field("common_1", ArrayType(DoubleType)),
                Field("common_2", ArrayType(VarcharType(10))),
                Field("common_3", ArrayType(DoubleType, false)),
                Field("left_1", ArrayType(IntegerType)),
                Field("left_2", ArrayType(IntegerType, false))
            )),
            StructType(Seq(
                Field("common_1", ArrayType(DoubleType)),
                Field("common_2", ArrayType(CharType(20))),
                Field("common_3", ArrayType(DoubleType, true)),
                Field("right_1", ArrayType(IntegerType)),
                Field("right_2", ArrayType(IntegerType, false))
            ))
        ) should be (
            StructType(Seq(
                Field("common_1", ArrayType(DoubleType)),
                Field("common_2", ArrayType(VarcharType(20))),
                Field("common_3", ArrayType(DoubleType, true)),
                Field("right_1", ArrayType(IntegerType)),
                Field("right_2", ArrayType(IntegerType, false), true),
                Field("left_1", ArrayType(IntegerType)),
                Field("left_2", ArrayType(IntegerType, false))
            ))
        )
    }

    it should "support structs" in {
        SchemaUtils.union(
            StructType(Seq(
                Field("common", StructType(Seq(
                    Field("common_1", DoubleType),
                    Field("common_2", VarcharType(10)),
                    Field("left_1", IntegerType),
                    Field("left_2", IntegerType, false)
                ))),
                Field("left_1", StructType(Seq(
                    Field("int", IntegerType)
                )))
            )),
            StructType(Seq(
                Field("common", StructType(Seq(
                    Field("common_1", DoubleType),
                    Field("common_2", CharType(20)),
                    Field("right_1", IntegerType),
                    Field("right_2", IntegerType, false)
                ))),
                Field("right_1", StructType(Seq(
                    Field("double", DoubleType)
                )))
            ))
        ) should be (
            StructType(Seq(
                Field("common", StructType(Seq(
                    Field("common_1", DoubleType),
                    Field("common_2", VarcharType(20)),
                    Field("left_1", IntegerType),
                    Field("left_2", IntegerType, true),
                    Field("right_1", IntegerType),
                    Field("right_2", IntegerType, true)
                ))),
                Field("right_1", StructType(Seq(
                    Field("double", DoubleType)
                ))),
                Field("left_1", StructType(Seq(
                    Field("int", IntegerType)
                )))
            ))
        )
    }
}
