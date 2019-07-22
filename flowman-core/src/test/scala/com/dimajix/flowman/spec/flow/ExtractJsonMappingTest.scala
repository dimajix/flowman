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
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.schema.SchemaSpec
import com.dimajix.flowman.spec.schema.SwaggerSchemaSpec
import com.dimajix.spark.testing.LocalSparkSession


class ExtractJsonMappingTest extends FlatSpec with Matchers with LocalSparkSession {
    "The ExtractJsonMapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  m0:
              |    kind: extractJson
              |    input: p0
              |    column: _1
              |    schema:
              |      fields:
              |        - name: s
              |          type: String
              |        - name: i
              |          type: Integer
              |        - name: st
              |          type:
              |            kind: struct
              |            fields:
              |             - name: lolo
              |               type: string
              |             - name: i
              |               type: Integer
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")

        project.mappings.size should be (1)
        project.mappings.contains("m0") should be (true)
        val mapping = project.mappings("m0")
        mapping shouldBe an[ExtractJsonMappingSpec]
    }

    it should "work with an explicit schema" in {
        val spec =
            """
              |mappings:
              |  m0:
              |    kind: extractJson
              |    input: p0
              |    column: _1
              |    schema:
              |      fields:
              |        - name: s
              |          type: String
              |        - name: i
              |          type: Integer
              |        - name: st
              |          type:
              |            kind: struct
              |            fields:
              |             - name: lolo
              |               type: string
              |             - name: i
              |               type: Integer
              |        - name: a
              |          type:
              |            kind: array
              |            elementType: Double
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val input = executor.spark.createDataFrame(Seq(
            ("""{"i":12,"s":"lala"}""", 12),
            ("""{"st":{"lolo":"x"},"a":[0.1,0.7]}""", 23)
        ))
        val inputSchema = com.dimajix.flowman.types.StructType.of(input.schema)

        val expectedSchema = StructType(
            StructField("s", StringType, true) ::
                StructField("i", IntegerType, true) ::
                StructField("st", StructType(
                    StructField("lolo", StringType, true) ::
                        StructField("i", IntegerType, true) ::
                        Nil
                )) ::
                StructField("a", ArrayType(DoubleType), true) ::
                Nil
        )

        val mapping = context.getMapping(MappingIdentifier("m0"))
        val result = mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input))("main")
        result.count() should be (2)
        result.schema should be (expectedSchema)

        val resultSchema = mapping.describe(Map(MappingOutputIdentifier("p0") -> inputSchema), "main")
        resultSchema.get.sparkType should be (expectedSchema)
    }

    it should "work without an explicit schema" in {
        val spec =
            """
              |mappings:
              |  m0:
              |    kind: extractJson
              |    input: p0
              |    column: _1
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val input = executor.spark.createDataFrame(Seq(
            ("""{"i":12,"s":"lala"}""", 12),
            ("""{"st":{"lolo":"x"},"a":[0.1,0.7]}""", 23)
        ))
        val inputSchema = com.dimajix.flowman.types.StructType.of(input.schema)

        val expectedSchema = StructType(
            StructField("a", ArrayType(DoubleType, true), true) ::
                StructField("i", LongType, true) ::
                StructField("s", StringType, true) ::
                StructField("st", StructType(
                    StructField("lolo", StringType, true) ::
                        Nil
                )) ::
                Nil
        )

        val mapping = context.getMapping(MappingIdentifier("m0"))
        val result = mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input))("main")
        result.count() should be (2)
        result.schema should be (expectedSchema)

        val resultSchema = mapping.describe(Map(MappingOutputIdentifier("p0") -> inputSchema), "main")
        resultSchema.get.sparkType should be (expectedSchema)
    }

    it should "work with invalid data" in {
        val spec =
            """
              |mappings:
              |  m0:
              |    kind: extractJson
              |    input: p0
              |    column: _1
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val input = executor.spark.createDataFrame(Seq(
            ("""invalid_json""", 12),
            ("""{"st":{"lolo":"x"},"a":[0.1,0.7]}""", 23)
        ))

        val mapping = context.getMapping(MappingIdentifier("m0"))
        val result = mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input))("main")
        result.count() should be (2)
        result.schema should be (StructType(
            StructField("_corrupt_record", StringType, true) ::
            StructField("a", ArrayType(DoubleType, true), true) ::
                StructField("st", StructType(
                    StructField("lolo", StringType, true) ::
                        Nil
                )) ::
                Nil
        ))
    }

    it should "work with invalid data with explicit schema" in {
        val spec =
            """
              |mappings:
              |  m0:
              |    kind: extractJson
              |    input: p0
              |    column: _1
              |    schema:
              |      fields:
              |        - name: s
              |          type: String
              |        - name: i
              |          type: Integer
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val input = executor.spark.createDataFrame(Seq(
            ("""invalid_json""", 12),
            ("""{"st":{"lolo":"x"},"a":[0.1,0.7]}""", 23)
        ))

        val mapping = context.getMapping(MappingIdentifier("m0"))
        val result = mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input))("main")
        result.count() should be (2)
        result.schema should be (StructType(
            StructField("s", StringType, true) ::
            StructField("i", IntegerType, true) ::
            Nil
        ))
    }

    it should "ignore records with invalid data" in {
        val spec =
            """
              |mappings:
              |  m0:
              |    kind: extractJson
              |    input: p0
              |    column: _1
              |    parseMode: DROPMALFORMED
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val input = executor.spark.createDataFrame(Seq(
            ("""invalid_json""", 12),
            ("""{"st":{"lolo":"x"},"a":[0.1,0.7]}""", 23)
        ))

        val mapping = context.getMapping(MappingIdentifier("m0"))
        val result = mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input))("main")
        result.count() should be (1)
        result.schema should be (StructType(
            StructField("a", ArrayType(DoubleType, true), true) ::
            StructField("st", StructType(
                StructField("lolo", StringType, true) ::
                    Nil
            )) ::
            Nil
        ))
    }

    it should "ignore records with invalid data and explicit schema" in {
        val spec =
            """
              |mappings:
              |  m0:
              |    kind: extractJson
              |    input: p0
              |    column: _1
              |    parseMode: DROPMALFORMED
              |    schema:
              |      fields:
              |        - name: s
              |          type: String
              |        - name: i
              |          type: Integer
              |            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val input = executor.spark.createDataFrame(Seq(
            ("""invalid_json""", 12),
            ("""{"st":{"lolo":"x"},"a":[0.1,0.7]}""", 23)
        ))

        val mapping = context.getMapping(MappingIdentifier("m0"))
        val result = mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input))("main")
        result.count() should be (1)
        result.schema should be (StructType(
            StructField("s", StringType, true) ::
            StructField("i", IntegerType, true) ::
            Nil
        ))
    }

    it should "fail on invalid data" in {
        val spec =
            """
              |mappings:
              |  m0:
              |    kind: extractJson
              |    input: p0
              |    column: _1
              |    parseMode: failfast
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val input = executor.spark.createDataFrame(Seq(
            ("""invalid_json""", 12),
            ("""{"st":{"lolo":"x"},"a":[0.1,0.7]}""", 23)
        ))

        val mapping = context.getMapping(MappingIdentifier("m0"))
        an[Exception] shouldBe thrownBy(mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input)))
    }

    it should "fail on invalid data with an explicit schema" in {
        val spec =
            """
              |mappings:
              |  m0:
              |    kind: extractJson
              |    input: p0
              |    column: _1
              |    parseMode: failfast
              |    schema:
              |      fields:
              |        - name: s
              |          type: String
              |        - name: i
              |          type: Integer
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val input = executor.spark.createDataFrame(Seq(
            ("""invalid_json""", 12),
            ("""{"st":{"lolo":"x"},"a":[0.1,0.7]}""", 23)
        ))

        val mapping = context.getMapping(MappingIdentifier("m0"))
        val result = mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input))("main")
        an[Exception] shouldBe thrownBy(result.count())
    }
}
