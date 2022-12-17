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

import scala.collection.JavaConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.spark.testing.LocalSparkSession


class JoinMappingTest extends AnyFlatSpec with Matchers with LocalSparkSession{
    "The JoinMapping" should "support joining on columns" in {
        val df1 = spark.createDataFrame(Seq(
                Row("col1", 12),
                Row("col2", 23),
                Row("col3", 34)
            ).asJava,
            StructType(
                StructField("key", StringType) ::
                StructField("lval", IntegerType) ::
                Nil
            )
        )
        val df2 = spark.createDataFrame(Seq(
                Row("col1", 32),
                Row("col2", 43),
                Row("col4", 43)
            ).asJava,
            StructType(
                StructField("key", StringType) ::
                StructField("rval", IntegerType) ::
                Nil
            )
        )

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val mapping = JoinMapping(
            Mapping.Properties(session.context),
            Seq(MappingOutputIdentifier("df1"), MappingOutputIdentifier("df2")),
            Seq("key"),
            mode="left"
        )
        mapping.inputs should be (Set(MappingOutputIdentifier("df1"), MappingOutputIdentifier("df2")))
        mapping.columns should be (Seq("key" ))

        val resultDf = mapping.execute(executor, Map(MappingOutputIdentifier("df1") -> df1, MappingOutputIdentifier("df2") -> df2))("main")
            .orderBy("key")
        val result = resultDf.collect()
        result.size should be (3)
        result(0) should be (Row("col1", 12, 32))
        result(1) should be (Row("col2", 23, 43))
        result(2) should be (Row("col3", 34, null))

        mapping.describe(executor, Map(
            MappingOutputIdentifier("df1") -> com.dimajix.flowman.types.StructType.of(df1.schema),
            MappingOutputIdentifier("df2") -> com.dimajix.flowman.types.StructType.of(df2.schema)
        )).map {case(k,v) => k -> v.sparkType } should be (Map(
            "main" -> resultDf.schema
        ))

        session.shutdown()
    }

    it should "support joining with an condition" in {
        val df1 = spark.createDataFrame(Seq(
                Row("col1", 12),
                Row("col2", 23),
                Row("col3", 34)
            ).asJava,
            StructType(
                StructField("key", StringType) ::
                StructField("lval", IntegerType) ::
                Nil
            )
        )
        val df2 = spark.createDataFrame(Seq(
                Row("col1", 32),
                Row("col2", 43),
                Row("col4", 43)
            ).asJava,
            StructType(
                StructField("key", StringType) ::
                StructField("rval", IntegerType) ::
                Nil
            )
        )

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val mapping = JoinMapping(
            Mapping.Properties(session.context),
            Seq(MappingOutputIdentifier("df1"), MappingOutputIdentifier("df2")),
            condition="df1.key = df2.key",
            mode="left"
        )
        mapping.inputs should be (Set(MappingOutputIdentifier("df1"), MappingOutputIdentifier("df2")))
        mapping.condition should be ("df1.key = df2.key")

        val resultDf = mapping.execute(executor, Map(MappingOutputIdentifier("df1") -> df1, MappingOutputIdentifier("df2") -> df2))("main")
            .orderBy("df1.key")
        val result = resultDf.collect()
        result.size should be (3)
        result(0) should be (Row("col1", 12, "col1", 32))
        result(1) should be (Row("col2", 23, "col2", 43))
        result(2) should be (Row("col3", 34, null, null))

        mapping.describe(executor, Map(
            MappingOutputIdentifier("df1") -> com.dimajix.flowman.types.StructType.of(df1.schema),
            MappingOutputIdentifier("df2") -> com.dimajix.flowman.types.StructType.of(df2.schema)
        )).map {case(k,v) => k -> v.sparkType } should be (Map(
            "main" -> resultDf.schema
        ))

        session.shutdown()
    }

    it should "be parseable" in {
        val spec =
            """
              |kind: join
              |inputs:
              |  - df1
              |  - df2
              |condition: "df1.key = df2.key"
            """.stripMargin
        val session = Session.builder().disableSpark().build()
        val mapping = ObjectMapper.parse[MappingSpec](spec)
        mapping shouldBe a[JoinMappingSpec]

        val join = mapping.instantiate(session.context).asInstanceOf[JoinMapping]
        join.inputs should be (Set(MappingOutputIdentifier("df1"), MappingOutputIdentifier("df2")))
        join.condition should be ("df1.key = df2.key")

        session.shutdown()
    }
}
