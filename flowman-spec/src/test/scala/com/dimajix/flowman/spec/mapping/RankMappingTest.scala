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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.mapping.RankMappingTest.Record
import com.dimajix.spark.sql.catalyst.SqlBuilder
import com.dimajix.spark.testing.LocalSparkSession


object RankMappingTest {
    case class Record(ts:(String,Long), id:(String,Int), data:String)
}

class RankMappingTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    implicit class Unqoute(str:String) {
        def unqouted : String = str.replace("`", "")
    }

    "The RankMapping" should "extract the latest version" in {
        val spark = this.spark
        import spark.implicits._

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val json_1 = Seq(
            """{"ts":123,"id":12, "a":[12,2], "op":"CREATE"}""",
            """{"ts":125,"id":12, "a":[12,2], "op":"UPDATE"}""",
            """{"ts":133,"id":12, "a":[12,3], "op":"UPDATE"}""",
            """{"ts":134,"id":12, "a":[12,4], "op":"DELETE"}""",
            """{"ts":123,"id":13, "a":[13,2], "op":"CREATE"}""",
            """{"ts":123,"id":14, "a":[14,2], "op":"CREATE"}""",
            """{"ts":124,"id":14, "a":[14,3], "op":"UPDATE"}""",
            """{"ts":127,"id":15, "a":[15,2], "op":"CREATE"}"""
        ).toDS
        val df = spark.read.json(json_1)

        val mapping = RankMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("df1"),
            Seq("id"),
            Seq("ts"),
            RankMapping.Latest
        )
        mapping.input should be (MappingOutputIdentifier("df1"))
        mapping.keyColumns should be (Seq("id" ))
        mapping.versionColumns should be (Seq("ts"))
        mapping.inputs should be (Set(MappingOutputIdentifier("df1")))

        val result = mapping.execute(executor, Map(MappingOutputIdentifier("df1") -> df))("main")
        result.schema should be (df.schema)

        val rows = result.orderBy("id").collect()
        rows.size should be (4)
        rows(0) should be (Row(mutable.WrappedArray.make(Array(12,4)), 12, "DELETE", 134))
        rows(1) should be (Row(mutable.WrappedArray.make(Array(13,2)), 13, "CREATE", 123))
        rows(2) should be (Row(mutable.WrappedArray.make(Array(14,3)), 14, "UPDATE", 124))
        rows(3) should be (Row(mutable.WrappedArray.make(Array(15,2)), 15, "CREATE", 127))

        session.shutdown()
    }

    it should "extract the earliest version" in {
        val spark = this.spark
        import spark.implicits._

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val json_1 = Seq(
            """{"ts":123,"id":12, "a":[12,2], "op":"CREATE"}""",
            """{"ts":125,"id":12, "a":[12,2], "op":"UPDATE"}""",
            """{"ts":133,"id":12, "a":[12,3], "op":"UPDATE"}""",
            """{"ts":134,"id":12, "a":[12,4], "op":"DELETE"}""",
            """{"ts":123,"id":13, "a":[13,2], "op":"CREATE"}""",
            """{"ts":123,"id":14, "a":[14,2], "op":"CREATE"}""",
            """{"ts":124,"id":14, "a":[14,3], "op":"UPDATE"}"""
        ).toDS
        val df = spark.read.json(json_1)

        val mapping = RankMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("df1"),
            Seq("id"),
            Seq("ts"),
            RankMapping.Earliest
        )
        mapping.input should be (MappingOutputIdentifier("df1"))
        mapping.keyColumns should be (Seq("id" ))
        mapping.versionColumns should be (Seq("ts"))
        mapping.inputs should be (Set(MappingOutputIdentifier("df1")))

        val result = mapping.execute(executor, Map(MappingOutputIdentifier("df1") -> df))("main")
        result.schema should be (df.schema)

        val rows = result.orderBy("id").collect()
        rows.size should be (3)
        rows(0) should be (Row(mutable.WrappedArray.make(Array(12,2)), 12, "CREATE", 123))
        rows(1) should be (Row(mutable.WrappedArray.make(Array(13,2)), 13, "CREATE", 123))
        rows(2) should be (Row(mutable.WrappedArray.make(Array(14,2)), 14, "CREATE", 123))

        session.shutdown()
    }

    it should "support the same version number multiple times" in {
        val spark = this.spark
        import spark.implicits._

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val json_1 = Seq(
            """{"ts":123,"id":12, "a":[12,1], "op":"CREATE"}""",
            """{"ts":125,"id":12, "a":[12,2], "op":"UPDATE"}""",
            """{"ts":133,"id":12, "a":[12,3], "op":"UPDATE"}""",
            """{"ts":133,"id":12, "a":[12,3], "op":"UPDATE"}""",
            """{"ts":123,"id":13, "a":[13,2], "op":"CREATE"}"""
        ).toDS
        val df = spark.read.json(json_1)

        val mapping = RankMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("df1"),
            Seq("id"),
            Seq("ts"),
            RankMapping.Latest
        )
        val result = mapping.execute(executor, Map(MappingOutputIdentifier("df1") -> df))("main")
        result.schema should be (df.schema)

        val rows = result.orderBy("id").collect()
        rows.size should be (2)
        rows(0) should be (Row(mutable.WrappedArray.make(Array(12,3)), 12, "UPDATE", 133))
        rows(1) should be (Row(mutable.WrappedArray.make(Array(13,2)), 13, "CREATE", 123))

        session.shutdown()
    }

    it should "support nested columns" in {
        val spark = this.spark
        import spark.implicits._

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val df = Seq(
            Record(("ts_0", 123), ("id_0", 7), "lala")
        ).toDF

        val mapping = RankMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("df1"),
            Seq("id._1"),
            Seq("ts._2"),
            RankMapping.Latest
        )
        mapping.input should be (MappingOutputIdentifier("df1"))
        mapping.keyColumns should be (Seq("id._1" ))
        mapping.versionColumns should be (Seq("ts._2"))
        mapping.inputs should be (Set(MappingOutputIdentifier("df1")))

        val result = mapping.execute(executor, Map(MappingOutputIdentifier("df1") -> df))("main")
        result.schema should be (df.schema)

        val rows = result.orderBy("id._1").as[Record].collect()
        rows should be (Seq(
            Record(("ts_0", 123), ("id_0", 7), "lala")
        ))

        session.shutdown()
    }

    it should "be parseable as 'latest' mapping" in {
        val spec =
            """
              |kind: latest
              |input: df1
              |keyColumns:
              | - id
              |versionColumns: v
            """.stripMargin
        val mappingSpec = ObjectMapper.parse[MappingSpec](spec)
        mappingSpec shouldBe a[LatestMappingSpec]

        val session = Session.builder().disableSpark().build()
        val mapping = mappingSpec.instantiate(session.context)
        val latest = mapping.asInstanceOf[RankMapping]
        latest.input should be (MappingOutputIdentifier("df1"))
        latest.keyColumns should be (Seq("id"))
        latest.versionColumns should be (Seq("v"))

        session.shutdown()
    }

    it should "be parseable as 'earliest' mapping" in {
        val spec =
            """
              |kind: earliest
              |input: df1
              |keyColumns:
              | - id
              |versionColumns: v
            """.stripMargin
        val mappingSpec = ObjectMapper.parse[MappingSpec](spec)
        mappingSpec shouldBe a[EarliestMappingSpec]

        val session = Session.builder().disableSpark().build()
        val mapping = mappingSpec.instantiate(session.context)
        val latest = mapping.asInstanceOf[RankMapping]
        latest.input should be (MappingOutputIdentifier("df1"))
        latest.keyColumns should be (Seq("id"))
        latest.versionColumns should be (Seq("v"))

        session.shutdown()
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
              |  latest:
              |    kind: latest
              |    input: some_table
              |    keyColumns: col_0
              |    versionColumns: ts
              |""".stripMargin

        val session = Session.builder().withSparkSession(spark).build()
        val project = Module.read.string(spec).toProject("default")
        val context = session.getContext(project)
        val executor = session.execution

        val mapping = context.getMapping(MappingIdentifier("latest"))
        val  df = executor.instantiate(mapping, "main")

        val sql = new SqlBuilder(df).toSQL
        sql.unqouted should be ("SELECT gen_attr_0 AS col_0, gen_attr_1 AS col_1, gen_attr_2 AS ts FROM (SELECT col_0 AS gen_attr_0, col_1 AS gen_attr_1, ts AS gen_attr_2, row_number() OVER (PARTITION BY col_0 ORDER BY ts DESC) AS gen_attr_3 FROM default.some_table) AS gen_subquery_2 WHERE (gen_attr_3 = 1)")
        noException shouldBe thrownBy(spark.sql(sql))

        spark.sql("DROP TABLE some_table")

        session.shutdown()
    })
}
