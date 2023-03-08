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

package com.dimajix.flowman.transforms

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.SchemaMismatchException
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.spark.sql.SchemaUtils
import com.dimajix.spark.testing.LocalSparkSession


class SchemaEnforcerTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The ColumnMismatchPolicy" should "parse correctly" in {
        ColumnMismatchPolicy.ofString("IGNORE") should be (ColumnMismatchPolicy.IGNORE)
        ColumnMismatchPolicy.ofString("ignore") should be (ColumnMismatchPolicy.IGNORE)
        ColumnMismatchPolicy.ofString("ERROR") should be (ColumnMismatchPolicy.ERROR)
        ColumnMismatchPolicy.ofString("ADD_COLUMNS_OR_IGNORE") should be (ColumnMismatchPolicy.ADD_COLUMNS_OR_IGNORE)
        ColumnMismatchPolicy.ofString("ADD_COLUMNS_OR_ERROR") should be (ColumnMismatchPolicy.ADD_COLUMNS_OR_ERROR)
        ColumnMismatchPolicy.ofString("REMOVE_COLUMNS_OR_IGNORE") should be (ColumnMismatchPolicy.REMOVE_COLUMNS_OR_IGNORE)
        ColumnMismatchPolicy.ofString("REMOVE_COLUMNS_OR_ERROR") should be (ColumnMismatchPolicy.REMOVE_COLUMNS_OR_ERROR)
        ColumnMismatchPolicy.ofString("ADD_REMOVE_COLUMNS") should be (ColumnMismatchPolicy.ADD_REMOVE_COLUMNS)
        a[NullPointerException] shouldBe thrownBy(ColumnMismatchPolicy.ofString(null))
        an[IllegalArgumentException] shouldBe thrownBy(ColumnMismatchPolicy.ofString("NO_SUCH_MODE"))
    }

    it should "provide a toString method" in {
        ColumnMismatchPolicy.IGNORE.toString should be ("IGNORE")
        ColumnMismatchPolicy.ERROR.toString should be ("ERROR")
        ColumnMismatchPolicy.ADD_COLUMNS_OR_IGNORE.toString should be ("ADD_COLUMNS_OR_IGNORE")
        ColumnMismatchPolicy.ADD_COLUMNS_OR_ERROR.toString should be ("ADD_COLUMNS_OR_ERROR")
        ColumnMismatchPolicy.REMOVE_COLUMNS_OR_IGNORE.toString should be ("REMOVE_COLUMNS_OR_IGNORE")
        ColumnMismatchPolicy.REMOVE_COLUMNS_OR_ERROR.toString should be ("REMOVE_COLUMNS_OR_ERROR")
        ColumnMismatchPolicy.ADD_REMOVE_COLUMNS.toString should be ("ADD_REMOVE_COLUMNS")
    }

    it should "parse toString correctly" in {
        ColumnMismatchPolicy.ofString(ColumnMismatchPolicy.IGNORE.toString) should be (ColumnMismatchPolicy.IGNORE)
        ColumnMismatchPolicy.ofString(ColumnMismatchPolicy.ERROR.toString) should be (ColumnMismatchPolicy.ERROR)
        ColumnMismatchPolicy.ofString(ColumnMismatchPolicy.ADD_COLUMNS_OR_IGNORE.toString) should be (ColumnMismatchPolicy.ADD_COLUMNS_OR_IGNORE)
        ColumnMismatchPolicy.ofString(ColumnMismatchPolicy.ADD_COLUMNS_OR_ERROR.toString) should be (ColumnMismatchPolicy.ADD_COLUMNS_OR_ERROR)
        ColumnMismatchPolicy.ofString(ColumnMismatchPolicy.REMOVE_COLUMNS_OR_IGNORE.toString) should be (ColumnMismatchPolicy.REMOVE_COLUMNS_OR_IGNORE)
        ColumnMismatchPolicy.ofString(ColumnMismatchPolicy.REMOVE_COLUMNS_OR_ERROR.toString) should be (ColumnMismatchPolicy.REMOVE_COLUMNS_OR_ERROR)
        ColumnMismatchPolicy.ofString(ColumnMismatchPolicy.ADD_REMOVE_COLUMNS.toString) should be (ColumnMismatchPolicy.ADD_REMOVE_COLUMNS)
    }


    "The TypeMismatchPolicy" should "parse correctly" in {
        TypeMismatchPolicy.ofString("IGNORE") should be (TypeMismatchPolicy.IGNORE)
        TypeMismatchPolicy.ofString("ignore") should be (TypeMismatchPolicy.IGNORE)
        TypeMismatchPolicy.ofString("ERROR") should be (TypeMismatchPolicy.ERROR)
        TypeMismatchPolicy.ofString("CAST_COMPATIBLE_OR_ERROR") should be (TypeMismatchPolicy.CAST_COMPATIBLE_OR_ERROR)
        TypeMismatchPolicy.ofString("CAST_COMPATIBLE_OR_IGNORE") should be (TypeMismatchPolicy.CAST_COMPATIBLE_OR_IGNORE)
        TypeMismatchPolicy.ofString("CAST_ALWAYS") should be (TypeMismatchPolicy.CAST_ALWAYS)
        a[NullPointerException] shouldBe thrownBy(TypeMismatchPolicy.ofString(null))
        an[IllegalArgumentException] shouldBe thrownBy(TypeMismatchPolicy.ofString("NO_SUCH_MODE"))
    }

    it should "provide a toString method" in {
        TypeMismatchPolicy.IGNORE.toString should be ("IGNORE")
        TypeMismatchPolicy.ERROR.toString should be ("ERROR")
        TypeMismatchPolicy.CAST_COMPATIBLE_OR_ERROR.toString should be ("CAST_COMPATIBLE_OR_ERROR")
        TypeMismatchPolicy.CAST_COMPATIBLE_OR_IGNORE.toString should be ("CAST_COMPATIBLE_OR_IGNORE")
        TypeMismatchPolicy.CAST_ALWAYS.toString should be ("CAST_ALWAYS")
    }

    it should "parse toString correctly" in {
        TypeMismatchPolicy.ofString(TypeMismatchPolicy.IGNORE.toString) should be (TypeMismatchPolicy.IGNORE)
        TypeMismatchPolicy.ofString(TypeMismatchPolicy.ERROR.toString) should be (TypeMismatchPolicy.ERROR)
        TypeMismatchPolicy.ofString(TypeMismatchPolicy.CAST_COMPATIBLE_OR_ERROR.toString) should be (TypeMismatchPolicy.CAST_COMPATIBLE_OR_ERROR)
        TypeMismatchPolicy.ofString(TypeMismatchPolicy.CAST_COMPATIBLE_OR_IGNORE.toString) should be (TypeMismatchPolicy.CAST_COMPATIBLE_OR_IGNORE)
        TypeMismatchPolicy.ofString(TypeMismatchPolicy.CAST_ALWAYS.toString) should be (TypeMismatchPolicy.CAST_ALWAYS)
    }


    "A conforming schema" should "be generated for simple cases" in {
        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col3", IntegerType)
        ))
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))

        val xfs = SchemaEnforcer(requestedSchema)
        val columns = xfs.transform(inputSchema)
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = inputDf.select(columns:_*)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        )))
    }

    it should "support ColumnMismatchPolicy.ADD_REMOVE_COLUMNS" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchPolicy=ColumnMismatchPolicy.ADD_REMOVE_COLUMNS,
            typeMismatchPolicy=TypeMismatchPolicy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col3", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = xfs.transform(inputDf)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        )))
    }
    it should "support ColumnMismatchPolicy.IGNORE" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchPolicy=ColumnMismatchPolicy.IGNORE,
            typeMismatchPolicy=TypeMismatchPolicy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col3", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = xfs.transform(inputDf)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col3", IntegerType)
        )))
    }
    it should "support ColumnMismatchPolicy.ERROR (1)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchPolicy=ColumnMismatchPolicy.ERROR,
            typeMismatchPolicy=TypeMismatchPolicy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col4", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = xfs.transform(inputDf)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        )))
    }
    it should "support ColumnMismatchPolicy.ERROR (2)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchPolicy=ColumnMismatchPolicy.ERROR,
            typeMismatchPolicy=TypeMismatchPolicy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col3", IntegerType),
            StructField("col4", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        a[SchemaMismatchException] should be thrownBy(xfs.transform(inputDf))
    }
    it should "support ColumnMismatchPolicy.ERROR (3)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchPolicy=ColumnMismatchPolicy.ERROR,
            typeMismatchPolicy=TypeMismatchPolicy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        a[SchemaMismatchException] should be thrownBy(xfs.transform(inputDf))
    }

    it should "support ColumnMismatchPolicy.ADD_COLUMNS_OR_IGNORE (1)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col3", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchPolicy=ColumnMismatchPolicy.ADD_COLUMNS_OR_IGNORE,
            typeMismatchPolicy=TypeMismatchPolicy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col4", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = xfs.transform(inputDf)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col3", StringType),
            StructField("col4", IntegerType)
        )))
    }
    it should "support ColumnMismatchPolicy.ADD_COLUMNS_OR_IGNORE (2)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchPolicy=ColumnMismatchPolicy.ADD_COLUMNS_OR_IGNORE,
            typeMismatchPolicy=TypeMismatchPolicy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col3", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = xfs.transform(inputDf)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType),
            StructField("col3", IntegerType)
        )))
    }

    it should "support ColumnMismatchPolicy.ADD_COLUMNS_OR_ERROR (1)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col3", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchPolicy=ColumnMismatchPolicy.ADD_COLUMNS_OR_ERROR,
            typeMismatchPolicy=TypeMismatchPolicy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col4", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = xfs.transform(inputDf)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col3", StringType),
            StructField("col4", IntegerType)
        )))
    }
    it should "support ColumnMismatchPolicy.ADD_COLUMNS_OR_ERROR (2)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchPolicy=ColumnMismatchPolicy.ADD_COLUMNS_OR_ERROR,
            typeMismatchPolicy=TypeMismatchPolicy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col3", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        a[SchemaMismatchException] should be thrownBy(xfs.transform(inputDf))
    }

    it should "support ColumnMismatchPolicy.REMOVE_COLUMNS_OR_IGNORE (1)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col3", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchPolicy=ColumnMismatchPolicy.REMOVE_COLUMNS_OR_IGNORE,
            typeMismatchPolicy=TypeMismatchPolicy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col4", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = xfs.transform(inputDf)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        )))
    }
    it should "support ColumnMismatchPolicy.REMOVE_COLUMNS_OR_IGNORE (2)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col3", StringType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchPolicy=ColumnMismatchPolicy.REMOVE_COLUMNS_OR_IGNORE,
            typeMismatchPolicy=TypeMismatchPolicy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col3", IntegerType),
            StructField("col4", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = xfs.transform(inputDf)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col3", StringType)
        )))
    }

    it should "support ColumnMismatchPolicy.REMOVE_COLUMNS_OR_ERROR (1)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col3", StringType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchPolicy=ColumnMismatchPolicy.REMOVE_COLUMNS_OR_ERROR,
            typeMismatchPolicy=TypeMismatchPolicy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col3", StringType),
            StructField("col4", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = xfs.transform(inputDf)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col3", StringType)
        )))
    }
    it should "support ColumnMismatchPolicy.REMOVE_COLUMNS_OR_ERROR (2)" in {
        val requestedSchema = StructType(Seq(
            StructField("col2", StringType),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))
        val xfs = SchemaEnforcer(
            requestedSchema,
            columnMismatchPolicy=ColumnMismatchPolicy.REMOVE_COLUMNS_OR_ERROR,
            typeMismatchPolicy=TypeMismatchPolicy.CAST_ALWAYS
        )

        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", IntegerType),
            StructField("col3", IntegerType)
        ))
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        a[SchemaMismatchException] should be thrownBy(xfs.transform(inputDf))
    }

    it should "work with nested entities" in {
        val inputSchema = StructType(Seq(
            StructField("col1", StringType),
            StructField("COL2", StructType(
                Seq(
                    StructField("nested1", StringType),
                    StructField("nested3", FloatType),
                    StructField("nested4", StructType(
                        Seq(
                            StructField("nested4_1", StringType),
                            StructField("nested4_2", FloatType)
                        )
                    )),
                    StructField("nested5", StructType(
                        Seq(
                            StructField("nested5_1", StringType),
                            StructField("nested5_2", FloatType)
                        )
                    ))
                )
            )),
            StructField("col3", IntegerType)
        ))
        val requestedSchema = StructType(Seq(
            StructField("col2", StructType(
                Seq(
                    StructField("nested1", LongType),
                    StructField("nested2", FloatType),
                    StructField("nested4", StructType(
                        Seq(
                            StructField("nested4_1", StringType),
                            StructField("nested4_3", FloatType)
                        )
                    )),
                    StructField("nested5", StructType(
                        Seq(
                            StructField("nested5_1", StringType),
                            StructField("nested5_2", FloatType)
                        )
                    ))
                )
            )),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        ))

        val xfs = SchemaEnforcer(requestedSchema)
        val columns = xfs.transform(inputSchema)
        val inputDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], inputSchema)
        val outputDf = inputDf.select(columns:_*)
        outputDf.schema should be (StructType(Seq(
            StructField("col2", StructType(
                Seq(
                    StructField("nested1", LongType),
                    StructField("nested2", FloatType),
                    StructField("nested4", StructType(
                        Seq(
                            StructField("nested4_1", StringType),
                            StructField("nested4_3", FloatType)
                        )
                    )),
                    StructField("nested5", StructType(
                        Seq(
                            StructField("nested5_1", StringType),
                            StructField("nested5_2", FloatType)
                        )
                    ))
                )
            )),
            StructField("col1", StringType),
            StructField("col4", IntegerType)
        )))
    }

    "The SchemaEnforcer" should "support extended string attributes" in {
        val spark = this.spark
        import spark.implicits._

        val inputDf = spark.createDataFrame(Seq(
            ("col", "1"),
            ("col123", "2345")
        ))
            .withColumn("_1", col("_1").as("_1"))
            .withColumn("_2", col("_2").as("_2"))
            .withColumn("_5", col("_2").as("_2"))
        val inputSchema = inputDf.schema

        val requestedSchema = com.dimajix.flowman.types.StructType(Seq(
            Field("_1", FieldType.of("varchar(4)")),
            Field("_2", FieldType.of("char(2)")),
            Field("_3", FieldType.of("string"))
        ))
        val xfs = SchemaEnforcer(requestedSchema.catalogType)

        val columns = xfs.transform(inputSchema)
        val outputDf = inputDf.select(columns:_*)
        val recs = outputDf.orderBy(col("_1"), col("_2")).as[(String, String, Option[String])].collect()
        recs should be(Seq(
            ("col", "1 ", None),
            ("col1", "23", None)
        ))

        SchemaUtils.dropMetadata(outputDf.schema) should be (StructType(Seq(
            StructField("_1", StringType),
            StructField("_2", StringType),
            StructField("_3", StringType)
        )))
        com.dimajix.flowman.types.StructType.of(outputDf.schema) should be (com.dimajix.flowman.types.StructType(Seq(
            Field("_1", FieldType.of("varchar(4)")),
            Field("_2", FieldType.of("char(2)")),
            Field("_3", FieldType.of("string"))
        )))
    }

    it should "work with different StringCastStrategies" in {
        val spark = this.spark
        import spark.implicits._

        val inputDf = spark.createDataFrame(Seq(
            ("col", "1"),
            ("col123", "2345")
        ))
            .withColumn("_1", col("_1").as("_1"))
            .withColumn("_2", col("_2").as("_2"))
            .withColumn("_5", col("_2").as("_2"))
        val inputSchema = inputDf.schema

        val requestedSchema = com.dimajix.flowman.types.StructType(Seq(
            Field("_1", FieldType.of("varchar(4)")),
            Field("_2", FieldType.of("char(2)")),
            Field("_3", FieldType.of("string"))
        ))

        {
            val xfs = SchemaEnforcer(requestedSchema.catalogType, charVarcharPolicy = CharVarcharPolicy.PAD_AND_TRUNCATE)
            val columns = xfs.transform(inputSchema)
            val outputDf = inputDf.select(columns: _*)
            val recs = outputDf.orderBy(col("_1"), col("_2")).as[(String, String, Option[String])].collect()
            recs should be(Seq(
                ("col", "1 ", None),
                ("col1", "23", None)
            ))
        }

        {
            val xfs = SchemaEnforcer(requestedSchema.catalogType, charVarcharPolicy = CharVarcharPolicy.PAD)
            val columns = xfs.transform(inputSchema)
            val outputDf = inputDf.select(columns: _*)
            val recs = outputDf.orderBy(col("_1"), col("_2")).as[(String, String, Option[String])].collect()
            recs should be(Seq(
                ("col", "1 ", None),
                ("col123", "2345", None)
            ))
        }

        {
            val xfs = SchemaEnforcer(requestedSchema.catalogType, charVarcharPolicy = CharVarcharPolicy.TRUNCATE)
            val columns = xfs.transform(inputSchema)
            val outputDf = inputDf.select(columns: _*)
            val recs = outputDf.orderBy(col("_1"), col("_2")).as[(String, String, Option[String])].collect()
            recs should be(Seq(
                ("col", "1", None),
                ("col1", "23", None)
            ))
        }

        {
            val xfs = SchemaEnforcer(requestedSchema.catalogType, charVarcharPolicy = CharVarcharPolicy.IGNORE)
            val columns = xfs.transform(inputSchema)
            val outputDf = inputDf.select(columns: _*)
            val recs = outputDf.orderBy(col("_1"), col("_2")).as[(String, String, Option[String])].collect()
            recs should be(Seq(
                ("col", "1", None),
                ("col123", "2345", None)
            ))
        }
    }

    it should "support comments" in {
        val inputDf = spark.createDataFrame(Seq(
            ("col1", "12"),
            ("col2", "23")
        ))
            .withColumn("_1", col("_1").as("_1", new MetadataBuilder().putString("comment","This is _1 original").build()))
            .withColumn("_2", col("_2").as("_2", new MetadataBuilder().putString("comment","This is _2 original").build()))
            .withColumn("_5", col("_2").as("_2"))
        val inputSchema = inputDf.schema

        val requestedSchema = com.dimajix.flowman.types.StructType(Seq(
            Field("_1", FieldType.of("int"), description=None),
            Field("_2", FieldType.of("int"), description=Some("This is _2")),
            Field("_3", FieldType.of("int"), description=Some("This is _3"))
        ))
        val xfs = SchemaEnforcer(requestedSchema.catalogType)

        val columns = xfs.transform(inputSchema)
        val outputDf = inputDf.select(columns:_*)
        outputDf.schema should be (com.dimajix.flowman.types.StructType(Seq(
            Field("_1", FieldType.of("int"), description=Some("This is _1 original")),
            Field("_2", FieldType.of("int"), description=Some("This is _2")),
            Field("_3", FieldType.of("int"), description=Some("This is _3"))
        )).catalogType)
        com.dimajix.flowman.types.StructType.of(outputDf.schema) should be (com.dimajix.flowman.types.StructType(Seq(
            Field("_1", FieldType.of("int"), description=Some("This is _1 original")),
            Field("_2", FieldType.of("int"), description=Some("This is _2")),
            Field("_3", FieldType.of("int"), description=Some("This is _3"))
        )))
    }

    it should "support collations" in {
        val inputDf = spark.createDataFrame(Seq(
            ("col1", "12", "33")
        ))
            .withColumn("_1", col("_1").as("_1"))
            .withColumn("_2", col("_2").as("_2", new MetadataBuilder().putString("collation","_2_orig").build()))
            .withColumn("_3", col("_3").as("_3", new MetadataBuilder().putString("collation","_3_orig").build()))
            .withColumn("_5", col("_3").as("_3"))
        val inputSchema = inputDf.schema

        val requestedSchema = com.dimajix.flowman.types.StructType(Seq(
            Field("_1", FieldType.of("string"), collation=None),
            Field("_2", FieldType.of("string"), collation=Some("_2")),
            Field("_3", FieldType.of("string"), collation=None),
            Field("_4", FieldType.of("string"), collation=Some("_4"))
        ))

        val xfs = SchemaEnforcer(requestedSchema.catalogType)
        val columns = xfs.transform(inputSchema)
        val outputDf = inputDf.select(columns:_*)
        outputDf.schema should be (StructType(Seq(
            StructField("_1", StringType),
            StructField("_2", StringType, metadata=new MetadataBuilder().putString("collation","_2").build()),
            StructField("_3", StringType),
            StructField("_4", StringType, metadata=new MetadataBuilder().putString("collation","_4").build()))
        ))
        com.dimajix.flowman.types.StructType.of(outputDf.schema) should be (com.dimajix.flowman.types.StructType(Seq(
            Field("_1", FieldType.of("string"), collation=None),
            Field("_2", FieldType.of("string"), collation=Some("_2")),
            Field("_3", FieldType.of("string"), collation=None),
            Field("_4", FieldType.of("string"), collation=Some("_4"))
        )))
    }
}
