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
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.VarcharType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.testing.LocalSparkSession
import com.dimajix.spark.testing.QueryTest


class SparkSchemaUtilsTest extends AnyFlatSpec with Matchers with LocalSparkSession with QueryTest {
    "SchemaUtils.merge" should "merge two fields" in {
        SparkSchemaUtils.merge(
            StructField("f1", IntegerType, true),
            StructField("f1", LongType, true)
        ) should be (
            StructField("f1", LongType, true)
        )

        SparkSchemaUtils.merge(
            StructField("f1", IntegerType, true),
            StructField("f1", StringType, true)
        ) should be (
            StructField("f1", StringType, true)
        )

        SparkSchemaUtils.merge(
            StructField("f1", IntegerType, true),
            StructField("f1", StringType, true)
        ) should be (
            StructField("f1", StringType, true)
        )
    }

    it should "pay attention to nullability" in {
        SparkSchemaUtils.merge(
            StructField("f1", IntegerType, true),
            StructField("f1", LongType, false)
        ) should be (
            StructField("f1", LongType, true)
        )

        SparkSchemaUtils.merge(
            StructField("f1", IntegerType, false),
            StructField("f1", LongType, true)
        ) should be (
            StructField("f1", LongType, true)
        )

        SparkSchemaUtils.merge(
            StructField("f1", IntegerType, false),
            StructField("f1", LongType, false)
        ) should be (
            StructField("f1", LongType, false)
        )
    }

    it should "pay attention to NullTypes" in {
        SparkSchemaUtils.merge(
            StructField("f1", NullType, false),
            StructField("f1", LongType, false)
        ) should be (
            StructField("f1", LongType, true)
        )

        SparkSchemaUtils.merge(
            StructField("f1", NullType, false),
            StructField("f1", LongType, false)
        ) should be (
            StructField("f1", LongType, true)
        )
    }


    "SchemaUtils.coerce" should "coerce two data types" in {
        SparkSchemaUtils.coerce(IntegerType, IntegerType) should be (IntegerType)
        SparkSchemaUtils.coerce(LongType, IntegerType) should be (LongType)

        SparkSchemaUtils.coerce(StringType, IntegerType) should be (StringType)
        SparkSchemaUtils.coerce(StringType, BooleanType) should be (StringType)
        SparkSchemaUtils.coerce(StringType, FloatType) should be (StringType)
        SparkSchemaUtils.coerce(StringType, DecimalType(20,10)) should be (StringType)

        SparkSchemaUtils.coerce(IntegerType, StringType) should be (StringType)
        SparkSchemaUtils.coerce(BooleanType, StringType) should be (StringType)
        SparkSchemaUtils.coerce(FloatType, StringType) should be (StringType)
        SparkSchemaUtils.coerce(DecimalType(20,10), StringType) should be (StringType)
    }

    it should "coerce null types" in {
        SparkSchemaUtils.coerce(IntegerType, NullType) should be (IntegerType)
        SparkSchemaUtils.coerce(BooleanType, NullType) should be (BooleanType)

        SparkSchemaUtils.coerce(NullType, IntegerType) should be (IntegerType)
        SparkSchemaUtils.coerce(NullType, BooleanType) should be (BooleanType)
    }

    it should "coerce integral and fractional types" in {
        SparkSchemaUtils.coerce(IntegerType, FloatType) should be (DoubleType)
        SparkSchemaUtils.coerce(LongType, FloatType) should be (DoubleType)
        SparkSchemaUtils.coerce(LongType, DoubleType) should be (DoubleType)

        SparkSchemaUtils.coerce(FloatType, IntegerType) should be (DoubleType)
        SparkSchemaUtils.coerce(FloatType, LongType) should be (DoubleType)
        SparkSchemaUtils.coerce(DoubleType, LongType) should be (DoubleType)
    }

    it should "coerce Boolean types" in {
        SparkSchemaUtils.coerce(BooleanType, BooleanType) should be (BooleanType)
        SparkSchemaUtils.coerce(LongType, BooleanType) should be (StringType)
        SparkSchemaUtils.coerce(DoubleType, BooleanType) should be (StringType)

        SparkSchemaUtils.coerce(BooleanType, LongType) should be (StringType)
        SparkSchemaUtils.coerce(BooleanType, DoubleType) should be (StringType)
    }

    it should "coerce Decimal types" in {
        SparkSchemaUtils.coerce(LongType, DecimalType(4,2)) should be (DecimalType(22,2))
        SparkSchemaUtils.coerce(IntegerType, DecimalType(4,2)) should be (DecimalType(12,2))
        SparkSchemaUtils.coerce(DoubleType, DecimalType(4,2)) should be (DoubleType)

        SparkSchemaUtils.coerce(DecimalType(4,2), LongType) should be (DecimalType(22,2))
        SparkSchemaUtils.coerce(DecimalType(4,2), IntegerType) should be (DecimalType(12,2))

        SparkSchemaUtils.coerce(DecimalType(10,1), DecimalType(4,2)) should be (DecimalType(11,2))
        SparkSchemaUtils.coerce(DecimalType(4,2), DecimalType(10,1)) should be (DecimalType(11,2))
    }

    it should "coerce VarChar and Char types" in {
        SparkSchemaUtils.coerce(VarcharType(10), VarcharType(10)) should be (VarcharType(10))
        SparkSchemaUtils.coerce(VarcharType(20), VarcharType(10)) should be (VarcharType(20))
        SparkSchemaUtils.coerce(VarcharType(10), VarcharType(20)) should be (VarcharType(20))

        SparkSchemaUtils.coerce(CharType(10), CharType(10)) should be (CharType(10))
        SparkSchemaUtils.coerce(VarcharType(20), VarcharType(10)) should be (VarcharType(20))
        SparkSchemaUtils.coerce(VarcharType(10), VarcharType(20)) should be (VarcharType(20))
    }

    "SchemaUtils.isCompatible" should "support decimal types" in {
        SparkSchemaUtils.isCompatible(StructField("f", DecimalType(4,2)), StructField("f", DecimalType(4,0))) should be (false)
        SparkSchemaUtils.isCompatible(StructField("f", DecimalType(4,2)), StructField("f", DecimalType(3,2))) should be (false)
        SparkSchemaUtils.isCompatible(StructField("f", DecimalType(4,2)), StructField("f", DecimalType(4,2))) should be (true)
        SparkSchemaUtils.isCompatible(StructField("f", DecimalType(4,2)), StructField("f", DecimalType(8,2))) should be (true)
        SparkSchemaUtils.isCompatible(StructField("f", DecimalType(4,2)), StructField("f", DecimalType(8,4))) should be (true)
        SparkSchemaUtils.isCompatible(StructField("f", DecimalType(4,2)), StructField("f", DecimalType(5,4))) should be (false)
    }

    it should "support nullability" in {
        SparkSchemaUtils.isCompatible(StructField("f", StringType, true), StructField("f", StringType, true)) should be (true)
        SparkSchemaUtils.isCompatible(StructField("f", StringType, false), StructField("f", StringType, true)) should be (true)
        SparkSchemaUtils.isCompatible(StructField("f", StringType, true), StructField("f", StringType, false)) should be (false)
        SparkSchemaUtils.isCompatible(StructField("f", StringType, false), StructField("f", StringType, false)) should be (true)
    }
}
