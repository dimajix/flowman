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

import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.testing.LocalSparkSession


class UnpackJsonMappingTest extends FlatSpec with Matchers with LocalSparkSession {
    "The UnpackJsonMapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  m0:
              |    kind: unpackJson
              |    input: p0
              |    columns:
              |      - name: _1
              |        schema:
              |          kind: embedded
              |          fields:
              |            - name: s
              |              type: String
              |            - name: i
              |              type: Integer
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")

        project.mappings.size should be(1)
        project.mappings.contains("m0") should be(true)
        project.mappings("m0") shouldBe an[UnpackJsonMappingSpec]
    }

    it should "work" in {
        val spec =
            """
              |mappings:
              |  m0:
              |    kind: unpackJson
              |    input: p0
              |    columns:
              |      - name: _1
              |        schema:
              |          kind: embedded
              |          fields:
              |            - name: s
              |              type: String
              |            - name: i
              |              type: Integer
              |            - name: st
              |              type:
              |                kind: struct
              |                fields:
              |                 - name: lolo
              |                   type: string
              |                 - name: i
              |                   type: Integer
              |            - name: a
              |              type:
              |                kind: array
              |                elementType: Double
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.getExecutor(project)

        val input = executor.spark.createDataFrame(Seq(
            ("""{"i":12,"s":"lala"}""", 12),
            ("""{"st":{"lolo":"x"},"a":[0.1,0.7]}""", 23)
        ))

        val mapping = executor.context.getMapping(MappingIdentifier("m0"))
        val result = mapping.execute(executor, Map(MappingIdentifier("p0") -> input))
        result.count() should be(2)
        result.schema should be(StructType(
            StructField("_1", StructType(
                StructField("s", StringType, true) ::
                    StructField("i", IntegerType, true) ::
                    StructField("st", StructType(
                        StructField("lolo", StringType, true) ::
                        StructField("i", IntegerType, true) ::
                        Nil
                    )) ::
                    StructField("a", ArrayType(DoubleType), true) ::
                    Nil
            )) ::
            StructField("_2", IntegerType, false) ::
            Nil
        ))
    }

    it should "support column renames" in {
        val spec =
            """
              |mappings:
              |  m0:
              |    kind: unpackJson
              |    input: p0
              |    columns:
              |      - name: _1
              |        alias: json
              |        schema:
              |          kind: embedded
              |          fields:
              |            - name: s
              |              type: String
              |            - name: i
              |              type: Integer
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.getExecutor(project)

        val input = executor.spark.createDataFrame(Seq(
            ("""{"i":12,"s":"lala"}""", 12),
            ("""{"st":{"lolo":"x"},"a":[0.1,0.7]}""", 23)
        ))

        val mapping = executor.context.getMapping(MappingIdentifier("m0"))
        val result = mapping.execute(executor, Map(MappingIdentifier("p0") -> input))
        result.count() should be(2)
        result.schema should be(StructType(
            StructField("_1", StringType, true) ::
            StructField("_2", IntegerType, false) ::
            StructField("json", StructType(
                StructField("s", StringType, true) ::
                    StructField("i", IntegerType, true) ::
                    Nil
            )) ::
            Nil
        ))
    }

    it should "nullify invalid record" in {
        val spec =
            """
              |mappings:
              |  m0:
              |    kind: unpackJson
              |    input: p0
              |    columns:
              |      - name: _1
              |        alias: json
              |        schema:
              |          kind: embedded
              |          fields:
              |            - name: s
              |              type: String
              |            - name: i
              |              type: Integer
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.getExecutor(project)

        val input = executor.spark.createDataFrame(Seq(
            ("""invalid_json""", 12),
            ("""{"st":{"lolo":"x"},"a":[0.1,0.7]}""", 23)
        ))

        val mapping = executor.context.getMapping(MappingIdentifier("m0"))
        val result = mapping.execute(executor, Map(MappingIdentifier("p0") -> input))
        result.count() should be(2)
        result.schema should be(StructType(
            StructField("_1", StringType, true) ::
            StructField("_2", IntegerType, false) ::
            StructField("json", StructType(
                StructField("s", StringType, true) ::
                    StructField("i", IntegerType, true) ::
                    Nil
            )) ::
            Nil
        ))
    }
}
