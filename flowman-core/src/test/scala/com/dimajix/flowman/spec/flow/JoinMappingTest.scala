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

package com.dimajix.flowman.spec.flow

import scala.collection.JavaConversions._

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.spark.testing.LocalSparkSession


class JoinMappingTest extends FlatSpec with Matchers with LocalSparkSession{
    "The JoinMapping" should "support joining on columns" in {
        val df1 = spark.createDataFrame(Seq(
            Row("col1", 12),
            Row("col2", 23),
            Row("col3", 34)
        ), StructType(
            StructField("key", StringType) ::
            StructField("lval", IntegerType) ::
            Nil
        ))
        val df2 = spark.createDataFrame(Seq(
            Row("col1", 32),
            Row("col2", 43),
            Row("col4", 43)
        ), StructType(
            StructField("key", StringType) ::
            StructField("rval", IntegerType) ::
            Nil
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = JoinMapping(
            Mapping.Properties(session.context),
            Seq(MappingOutputIdentifier("df1"), MappingOutputIdentifier("df2")),
            Seq("key"),
            mode="left"
        )
        mapping.inputs should be (Seq(MappingOutputIdentifier("df1"), MappingOutputIdentifier("df2")))
        mapping.columns should be (Seq("key" ))
        mapping.inputs should be (Seq(MappingOutputIdentifier("df1"), MappingOutputIdentifier("df2")))

        val result = mapping.execute(executor, Map(MappingOutputIdentifier("df1") -> df1, MappingOutputIdentifier("df2") -> df2))("main")
            .orderBy("key").collect()
        result.size should be (3)
        result(0) should be (Row("col1", 12, 32))
        result(1) should be (Row("col2", 23, 43))
        result(2) should be (Row("col3", 34, null))
    }

    it should "support joining with an condition" in {
        val df1 = spark.createDataFrame(Seq(
            Row("col1", 12),
            Row("col2", 23),
            Row("col3", 34)
        ), StructType(
            StructField("key", StringType) ::
                StructField("lval", IntegerType) ::
                Nil
        ))
        val df2 = spark.createDataFrame(Seq(
            Row("col1", 32),
            Row("col2", 43),
            Row("col4", 43)
        ), StructType(
            StructField("key", StringType) ::
                StructField("rval", IntegerType) ::
                Nil
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor

        val mapping = JoinMapping(
            Mapping.Properties(session.context),
            Seq(MappingOutputIdentifier("df1"), MappingOutputIdentifier("df2")),
            condition="df1.key = df2.key",
            mode="left"
        )
        mapping.inputs should be (Seq(MappingOutputIdentifier("df1"), MappingOutputIdentifier("df2")))
        mapping.condition should be ("df1.key = df2.key")
        mapping.inputs should be (Seq(MappingOutputIdentifier("df1"), MappingOutputIdentifier("df2")))

        val result = mapping.execute(executor, Map(MappingOutputIdentifier("df1") -> df1, MappingOutputIdentifier("df2") -> df2))("main")
            .orderBy("df1.key").collect()
        result.size should be (3)
        result(0) should be (Row("col1", 12, "col1", 32))
        result(1) should be (Row("col2", 23, "col2", 43))
        result(2) should be (Row("col3", 34, null, null))
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
        val session = Session.builder().build()
        val mapping = ObjectMapper.parse[MappingSpec](spec)
        mapping shouldBe a[JoinMappingSpec]

        val join = mapping.instantiate(session.context).asInstanceOf[JoinMapping]
        join.inputs should be (Seq(MappingOutputIdentifier("df1"), MappingOutputIdentifier("df2")))
        join.condition should be ("df1.key = df2.key")
        join.inputs should be (Seq(MappingOutputIdentifier("df1"), MappingOutputIdentifier("df2")))
    }
}
