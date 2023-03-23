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

package com.dimajix.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.CharType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.VarcharType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.testing.LocalSparkSession
import com.dimajix.spark.testing.QueryTest


class SchemaUtilsTest extends AnyFlatSpec with Matchers with LocalSparkSession with QueryTest {
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

    it should "truncate and pad VarChar / Char types" in {
        val desiredSchema = StructType(Seq(
            StructField("char_col", CharType(4)),
            StructField("varchar_col", VarcharType(4))
        ))
        val resultSchema = StructType(Seq(
            StructField("char_col", StringType),
            StructField("varchar_col", StringType)
        ))

        val inputSchema = StructType(Seq(
            StructField("char_col", StringType),
            StructField("varchar_col", LongType)
        ))
        val rows = spark.sparkContext.parallelize(Seq(
            Row(null, null),
            Row("234", 123l),
            Row("2345", 1234l),
            Row("23456", 12345l)
        ))
        val df = spark.createDataFrame(rows, inputSchema)

        val result = SchemaUtils.applySchema(df, Some(desiredSchema))
        result.schema should be (resultSchema)
        val expectedRows = Seq(
            Row(null, null),
            Row("234 ", "123"),
            Row("2345", "1234"),
            Row("2345", "1234")
        )
        checkAnswer(result, expectedRows)
    }

    "SchemaUtils.replaceCharVarchar" should "work with simple types" in {
        val originalSchema = StructType(Seq(
            StructField("char_col", CharType(4)),
            StructField("varchar_col", VarcharType(4), nullable=false)
        ))

        val replacedSchema = SchemaUtils.replaceCharVarchar(originalSchema)
        SchemaUtils.dropMetadata(replacedSchema) should be (StructType(Seq(
            StructField("char_col", StringType),
            StructField("varchar_col", StringType, nullable=false)
        )))

        val recoveredSchema = SchemaUtils.recoverCharVarchar(replacedSchema)
        recoveredSchema should be (originalSchema)
    }

    it should "work with nested types" in {
        val originalSchema = StructType(Seq(
            StructField("array_col", ArrayType(CharType(10))),
            StructField("map_col", MapType(CharType(15), VarcharType(20))),
            StructField("struct_col", StructType(Seq(
                StructField("char_field", CharType(10)),
                StructField("int_field", IntegerType)
            ))),
            StructField("varchar_col", VarcharType(4), nullable=false)
        ))

        val replacedSchema = SchemaUtils.replaceCharVarchar(originalSchema)
        SchemaUtils.dropMetadata(replacedSchema) should be (StructType(Seq(
            StructField("array_col", ArrayType(StringType)),
            StructField("map_col", MapType(StringType, StringType)),
            StructField("struct_col", StructType(Seq(
                StructField("char_field", StringType),
                StructField("int_field", IntegerType)
            ))),
            StructField("varchar_col", StringType, nullable=false)
        )))

        val recoveredSchema = SchemaUtils.recoverCharVarchar(replacedSchema)
        recoveredSchema should be (originalSchema)
    }

    it should "gracefully handle non-standard characters in field names" in {
        val originalSchema = StructType(Seq(
            StructField("array-col", ArrayType(CharType(10))),
            StructField("map:col", MapType(CharType(15), VarcharType(20))),
            StructField("struct col", StructType(Seq(
                StructField("char-field", CharType(10)),
                StructField("varchar:field", VarcharType(25)),
                StructField("int field", IntegerType),
                StructField("Mönster Fieldß", VarcharType(30))
            ))),
            StructField("varchar/col", VarcharType(4), nullable = false)
        ))

        val replacedSchema = SchemaUtils.replaceCharVarchar(originalSchema)
        SchemaUtils.dropMetadata(replacedSchema) should be(StructType(Seq(
            StructField("array-col", ArrayType(StringType)),
            StructField("map:col", MapType(StringType, StringType)),
            StructField("struct col", StructType(Seq(
                StructField("char-field", StringType),
                StructField("varchar:field", StringType),
                StructField("int field", IntegerType),
                StructField("Mönster Fieldß", StringType)
            ))),
            StructField("varchar/col", StringType, nullable = false)
        )))

        val recoveredSchema = SchemaUtils.recoverCharVarchar(replacedSchema)
        recoveredSchema should be(originalSchema)
    }

    "SchemaUtils.dropExtendedTypeInfo" should "remove only extended type info" in {
        val comment = "123456789"
        val schema = StructType(Seq(
            StructField("Name", VarcharType(50), false).withComment(comment),
            StructField("Nested",
                StructType(Seq(
                    StructField("AmOUnt", CharType(10)).withComment(comment),
                    StructField("SomeArray", ArrayType(VarcharType(20), false)).withComment(comment)
                ))
            ).withComment(comment),
            StructField("StructArray", ArrayType(
                StructType(Seq(
                    StructField("Name", StringType).withComment(comment)
                ))
            )).withComment(comment)
        ))

        val replacedSchema = SchemaUtils.replaceCharVarchar(schema)
        val pureSchema = SchemaUtils.dropExtendedTypeInfo(replacedSchema)

        val expectedSchema = StructType(Seq(
            StructField("Name", StringType, false).withComment(comment),
            StructField("Nested",
                StructType(Seq(
                    StructField("AmOUnt", StringType).withComment(comment),
                    StructField("SomeArray", ArrayType(StringType, false)).withComment(comment)
                ))
            ).withComment(comment),
            StructField("StructArray", ArrayType(
                StructType(Seq(
                    StructField("Name", StringType).withComment(comment)
                ))
            )).withComment(comment)
        ))
        pureSchema should be(expectedSchema)
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

    "SchemaUtils.normalize" should "convert all field names to lower case and remove all meta data except comments" in {
        val comment = "123456789"
        val metadata = new MetadataBuilder()
            .putString("comment", comment)
            .putString("meta", "123")
            .build()
        val schema = StructType(Seq(
            StructField("Name", StringType, false, metadata=metadata),
            StructField("Nested",
                StructType(Seq(
                    StructField("AmOUnt", DoubleType, metadata=metadata),
                    StructField("SomeArray", ArrayType(IntegerType, false), metadata=metadata)
                )), metadata=metadata
            ),
            StructField("StructArray", ArrayType(
                StructType(Seq(
                    StructField("Name", VarcharType(10), metadata=metadata)
                ))
            ), metadata=metadata)
        ))

        val pureSchema = SchemaUtils.normalize(schema)

        val expectedSchema = StructType(Seq(
            StructField("name", StringType, false).withComment(comment),
            StructField("nested",
                StructType(Seq(
                    StructField("amount", DoubleType).withComment(comment),
                    StructField("somearray", ArrayType(IntegerType, false)).withComment(comment)
                ))
            ).withComment(comment),
            StructField("structarray", ArrayType(
                StructType(Seq(
                    StructField("name", StringType).withComment(comment)
                ))
            )).withComment(comment)
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
}
