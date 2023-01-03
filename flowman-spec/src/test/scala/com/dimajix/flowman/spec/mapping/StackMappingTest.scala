/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.mapping

import java.sql.Date

import scala.collection.immutable.ListMap

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.{types => ftypes}
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.spark.testing.LocalSparkSession


class StackMappingTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The StackMapping" should "be parseable" in {
        val spec = """
          |kind: stack
          |input: translations
          |filter: en <> de
          |dropNulls: true
          |keepColumns:
          |  - product
          |  - sub_product
          |dropColumns:
          |  - product_description
          |  - sub_product_code
          |nameColumn: lang
          |valueColumn: text
          |stackColumns:
          |  EN Text: en
          |  German Text: de
          |  fr: fr
          |""".stripMargin

        val mappingSpec = ObjectMapper.parse[MappingSpec](spec)
        mappingSpec shouldBe a[StackMappingSpec]
    }

    it should "work" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val context = session.context
        val execution = session.execution

        val mapping = StackMapping(
            Mapping.Properties(context),
            MappingOutputIdentifier("input:main"),
            "col",
            "value",
            ListMap("_3" -> "col3", "_4" -> "col4"),
            dropNulls = false
        )

        val expectedSchema = StructType(Seq(
            StructField("_1", IntegerType, false),
            StructField("_2", StringType, true),
            StructField("_5", DateType, true),
            StructField("col", StringType, false),
            StructField("value", DoubleType, true)
        ))

        val df = spark.createDataFrame(Seq(
            (1,"lala", 33, Some(1.2), Date.valueOf("2020-01-02")),
            (2,"lolo", 44, None, Date.valueOf("2020-02-02"))
        ))
        val resultDf = mapping.execute(execution, Map(MappingOutputIdentifier("input:main") -> df))("main")
        resultDf.schema should be (expectedSchema)
        resultDf.count should be (4)
        resultDf.collect() should be (Seq(
            Row(1,"lala", Date.valueOf("2020-01-02"), "col3", 33.0),
            Row(2,"lolo", Date.valueOf("2020-02-02"), "col3", 44.0),
            Row(1,"lala", Date.valueOf("2020-01-02"), "col4", 1.2),
            Row(2,"lolo", Date.valueOf("2020-02-02"), "col4", null)
        ))

        val resultSchema = mapping.describe(execution, Map(MappingOutputIdentifier("input:main") -> ftypes.StructType.of(df.schema)))
        resultSchema should be (Map("main" -> ftypes.StructType.of(expectedSchema)))

        session.shutdown()
    }

    it should "support dropNulls" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val context = session.context
        val execution = session.execution

        val mapping = StackMapping(
            Mapping.Properties(context),
            MappingOutputIdentifier("input:main"),
            "col",
            "value",
            ListMap("_3" -> "col3", "_4" -> "col4"),
            dropNulls = true
        )

        val expectedSchema = StructType(Seq(
            StructField("_1", IntegerType, false),
            StructField("_2", StringType, true),
            StructField("_5", DateType, true),
            StructField("col", StringType, false),
            StructField("value", DoubleType, true)
        ))

        val df = spark.createDataFrame(Seq(
            (1,"lala", 33, Some(1.2), Date.valueOf("2020-01-02")),
            (2,"lolo", 44, None, Date.valueOf("2020-02-02"))
        ))
        val resultDf = mapping.execute(execution, Map(MappingOutputIdentifier("input:main") -> df))("main")
        resultDf.schema should be (expectedSchema)
        resultDf.count should be (3)
        resultDf.collect() should be (Seq(
            Row(1,"lala", Date.valueOf("2020-01-02"), "col3", 33.0),
            Row(2,"lolo", Date.valueOf("2020-02-02"), "col3", 44.0),
            Row(1,"lala", Date.valueOf("2020-01-02"), "col4", 1.2)
        ))

        val resultSchema = mapping.describe(execution, Map(MappingOutputIdentifier("input:main") -> ftypes.StructType.of(df.schema)))
        resultSchema should be (Map("main" -> ftypes.StructType.of(expectedSchema)))

        session.shutdown()
    }

    it should "support keepColumns" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val context = session.context
        val execution = session.execution

        val mapping = StackMapping(
            Mapping.Properties(context),
            MappingOutputIdentifier("input:main"),
            "col",
            "value",
            ListMap("_3" -> "col3", "_4" -> "col4"),
            dropNulls = false,
            keepColumns = Seq("_1")
        )

        val expectedSchema = StructType(Seq(
            StructField("_1", IntegerType, false),
            StructField("col", StringType, false),
            StructField("value", DoubleType, true)
        ))

        val df = spark.createDataFrame(Seq(
            (1,"lala", 33, Some(1.2), Date.valueOf("2020-01-02")),
            (2,"lolo", 44, None, Date.valueOf("2020-02-02"))
        ))
        val resultDf = mapping.execute(execution, Map(MappingOutputIdentifier("input:main") -> df))("main")
        resultDf.schema should be (expectedSchema)
        resultDf.count should be (4)
        resultDf.collect() should be (Seq(
            Row(1, "col3", 33.0),
            Row(2, "col3", 44.0),
            Row(1, "col4", 1.2),
            Row(2, "col4", null)
        ))

        val resultSchema = mapping.describe(execution, Map(MappingOutputIdentifier("input:main") -> ftypes.StructType.of(df.schema)))
        resultSchema should be (Map("main" -> ftypes.StructType.of(expectedSchema)))

        session.shutdown()
    }

    it should "support dropColumns" in {
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val context = session.context
        val execution = session.execution

        val mapping = StackMapping(
            Mapping.Properties(context),
            MappingOutputIdentifier("input:main"),
            "col",
            "value",
            ListMap("_3" -> "col3", "_4" -> "col4"),
            dropNulls = false,
            dropColumns = Seq("_2")
        )

        val expectedSchema = StructType(Seq(
            StructField("_1", IntegerType, false),
            StructField("_5", DateType, true),
            StructField("col", StringType, false),
            StructField("value", DoubleType, true)
        ))

        val df = spark.createDataFrame(Seq(
            (1,"lala", 33, Some(1.2), Date.valueOf("2020-01-02")),
            (2,"lolo", 44, None, Date.valueOf("2020-02-02"))
        ))
        val resultDf = mapping.execute(execution, Map(MappingOutputIdentifier("input:main") -> df))("main")
        resultDf.schema should be (expectedSchema)
        resultDf.count should be (4)
        resultDf.collect() should be (Seq(
            Row(1, Date.valueOf("2020-01-02"), "col3", 33.0),
            Row(2, Date.valueOf("2020-02-02"), "col3", 44.0),
            Row(1, Date.valueOf("2020-01-02"), "col4", 1.2),
            Row(2, Date.valueOf("2020-02-02"), "col4", null)
        ))

        val resultSchema = mapping.describe(execution, Map(MappingOutputIdentifier("input:main") -> ftypes.StructType.of(df.schema)))
        resultSchema should be (Map("main" -> ftypes.StructType.of(expectedSchema)))

        session.shutdown()
    }
}
