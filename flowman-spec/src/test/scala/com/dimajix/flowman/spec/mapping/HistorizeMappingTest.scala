/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

import scala.collection.mutable

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.spark.sql.catalyst.SqlBuilder
import com.dimajix.spark.testing.LocalSparkSession


class HistorizeMappingTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    implicit class Unqoute(str:String) {
        def unqouted : String = str.replace("`", "")
    }

    "The HistorizeMapping" should "extract the latest version" in {
        val spark = this.spark
        import spark.implicits._

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val json_1 = Seq(
            """{"ts":123,"id":12, "a":[12,2], "op":"CREATE"}""",
            """{"ts":125,"id":12, "a":[12,2], "op":"UPDATE"}""",
            """{"ts":133,"id":12, "a":[12,3], "op":"UPDATE"}""",
            """{"ts":134,"id":12, "a":[12,4], "op":"UPDATE"}""",
            """{"ts":123,"id":13, "a":[13,2], "op":"CREATE"}"""
        ).toDS
        val df = spark.read.json(json_1)

        val mapping = HistorizeMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("df1"),
            Seq("id"),
            "ts",
            Seq(),
            "valid_from",
            "valid_to"
        )
        mapping.input should be (MappingOutputIdentifier("df1"))
        mapping.inputs should be (Set(MappingOutputIdentifier("df1")))
        mapping.outputs should be (Set("main"))
        mapping.keyColumns should be (Seq("id" ))
        mapping.timeColumn should be ("ts")
        mapping.validFromColumn should be ("valid_from")
        mapping.validToColumn should be ("valid_to")

        val expectedSchema = StructType(Seq(
            StructField("a", ArrayType(LongType)),
            StructField("id", LongType),
            StructField("op", StringType),
            StructField("ts", LongType),
            StructField("valid_from", LongType),
            StructField("valid_to", LongType)
        ))

        val resultSchema = mapping.describe(executor, Map(MappingOutputIdentifier("df1") -> com.dimajix.flowman.types.StructType.of(df.schema)))("main")
        resultSchema should be (com.dimajix.flowman.types.StructType.of(expectedSchema))

        val result = mapping.execute(executor, Map(MappingOutputIdentifier("df1") -> df))("main")
        result.schema should be (expectedSchema)

        val rows = result.orderBy("id", "ts").collect()
        rows should be (Array(
            Row(mutable.WrappedArray.make(Array(12,2)), 12, "CREATE", 123, 123, 125),
            Row(mutable.WrappedArray.make(Array(12,2)), 12, "UPDATE", 125, 125, 133),
            Row(mutable.WrappedArray.make(Array(12,3)), 12, "UPDATE", 133, 133, 134),
            Row(mutable.WrappedArray.make(Array(12,4)), 12, "UPDATE", 134, 134, null),
            Row(mutable.WrappedArray.make(Array(13,2)), 13, "CREATE", 123, 123, null)
        ))
    }

    it should "support adding new columns at the beginning" in {
        val spark = this.spark
        import spark.implicits._

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val json_1 = Seq(
            """{"ts":123,"id":12}"""
        ).toDS
        val df = spark.read.json(json_1)

        val mapping = HistorizeMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("df1"),
            Seq("id"),
            "ts",
            Seq(),
            "valid_from",
            "valid_to",
            None,
            InsertPosition.BEGINNING
        )
        mapping.input should be (MappingOutputIdentifier("df1"))
        mapping.inputs should be (Set(MappingOutputIdentifier("df1")))
        mapping.outputs should be (Set("main"))
        mapping.keyColumns should be (Seq("id" ))
        mapping.timeColumn should be ("ts")
        mapping.validFromColumn should be ("valid_from")
        mapping.validToColumn should be ("valid_to")

        val expectedSchema = StructType(Seq(
            StructField("valid_from", LongType),
            StructField("valid_to", LongType),
            StructField("id", LongType),
            StructField("ts", LongType)
        ))

        val resultSchema = mapping.describe(executor, Map(MappingOutputIdentifier("df1") -> com.dimajix.flowman.types.StructType.of(df.schema)))("main")
        resultSchema should be (com.dimajix.flowman.types.StructType.of(expectedSchema))

        val result = mapping.execute(executor, Map(MappingOutputIdentifier("df1") -> df))("main")
        result.schema should be (expectedSchema)

        val rows = result.orderBy("id", "ts").collect()
        rows.size should be (1)
        rows(0) should be (Row(123, null, 12, 123))
    }

    it should "support versionColumns" in  {
        val spark = this.spark
        import spark.implicits._

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val json_1 = Seq(
            """{"version_major":1, "version_minor":1, "ts":123,"id":12, "a":[12,2], "op":"CREATE"}""",
            """{"version_major":1, "version_minor":2, "ts":125,"id":12, "a":[12,2], "op":"UPDATE"}""",
            """{"version_major":2, "version_minor":1, "ts":133,"id":12, "a":[12,3], "op":"UPDATE"}""",
            """{"version_major":3, "version_minor":1, "ts":134,"id":12, "a":[12,4], "op":"UPDATE"}""",
            """{"version_major":1, "version_minor":1, "ts":123,"id":13, "a":[13,2], "op":"CREATE"}"""
        ).toDS
        val df = spark.read.json(json_1)

        val mapping = HistorizeMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("df1"),
            Seq("id"),
            "ts",
            Seq("version_major", "version_minor"),
            "valid_from",
            "valid_to"
        )

        val expectedSchema = StructType(Seq(
            StructField("a", ArrayType(LongType)),
            StructField("id", LongType),
            StructField("op", StringType),
            StructField("ts", LongType),
            StructField("version_major", LongType),
            StructField("version_minor", LongType),
            StructField("valid_from", LongType),
            StructField("valid_to", LongType)
        ))

        val resultSchema = mapping.describe(executor, Map(MappingOutputIdentifier("df1") -> com.dimajix.flowman.types.StructType.of(df.schema)))("main")
        resultSchema should be (com.dimajix.flowman.types.StructType.of(expectedSchema))

        val result = mapping.execute(executor, Map(MappingOutputIdentifier("df1") -> df))("main")
        result.schema should be (expectedSchema)

        val rows = result.orderBy("id", "ts").collect()
        rows should be (Array(
            Row(mutable.WrappedArray.make(Array(12,2)), 12, "CREATE", 123, 1, 1, 123, 125),
            Row(mutable.WrappedArray.make(Array(12,2)), 12, "UPDATE", 125, 1, 2, 125, 133),
            Row(mutable.WrappedArray.make(Array(12,3)), 12, "UPDATE", 133, 2, 1, 133, 134),
            Row(mutable.WrappedArray.make(Array(12,4)), 12, "UPDATE", 134, 3, 1, 134, null),
            Row(mutable.WrappedArray.make(Array(13,2)), 13, "CREATE", 123, 1, 1, 123, null)
        ))

    }

    it should "be convertible to SQL" in (if (hiveSupported) {
        spark.sql(
            """
              CREATE TABLE some_table(
                col_0 INT,
                col_1 STRING,
                ts TIMESTAMP
              )
            """)

        val spec =
            """
              |relations:
              |  some_table:
              |    kind: hiveTable
              |    table: some_table
              |
              |mappings:
              |  some_table:
              |    kind: relation
              |    relation: some_table
              |
              |  history:
              |    kind: historize
              |    input: some_table
              |    keyColumns: col_0
              |    timeColumn: ts
              |    validFromColumn: validFrom
              |    validToColumn: validTo
              |""".stripMargin

        val session = Session.builder().withSparkSession(spark).build()
        val project = Module.read.string(spec).toProject("default")
        val context = session.getContext(project)
        val executor = session.execution

        val mapping = context.getMapping(MappingIdentifier("history"))
        val  df = executor.instantiate(mapping, "main")

        val sql = new SqlBuilder(df).toSQL
        sql.unqouted should be ("SELECT col_0, col_1, ts, ts AS validFrom, lead(ts, 1, NULL) OVER (PARTITION BY col_0 ORDER BY ts ASC) AS validTo FROM default.some_table")
        noException shouldBe thrownBy(spark.sql(sql))

        spark.sql("DROP TABLE some_table")
    })
}
