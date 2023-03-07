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

import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.{types => ftypes}
import com.dimajix.spark.testing.LocalSparkSession


class ExtractJsonMappingTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The ExtractJsonMapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  m0:
              |    kind: extractJson
              |    input: p0
              |    column: _1
              |    schema:
              |      kind: inline
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
              |      kind: inline
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
        val executor = session.execution
        val context = session.getContext(project)

        val input = executor.spark.createDataFrame(Seq(
            ("""{"i":12,"s":"lala"}""", 12),
            ("""{"st":{"lolo":"x"},"a":[0.1,0.7]}""", 23)
        ))
        val inputSchema = ftypes.StructType.of(input.schema)

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
        val errorSchema = StructType(
            StructField("record", StringType, false) :: Nil
        )

        val mapping = context.getMapping(MappingIdentifier("m0"))
        mapping.outputs should be (Set("main", "error"))

        val result = mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input))("main")
        result.count() should be (2)
        result.schema should be (expectedSchema)

        val errors = mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input))("error")
        errors.count() should be (0)
        errors.schema should be (errorSchema)

        val resultSchema = mapping.describe(executor, Map(MappingOutputIdentifier("p0") -> inputSchema))
        resultSchema should be (Map(
            "main" -> ftypes.StructType.of(expectedSchema),
            "error" -> ftypes.StructType.of(errorSchema)
        ))

        session.shutdown()
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
        val executor = session.execution
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
        val errorSchema = StructType(
            StructField("record", StringType, false) :: Nil
        )

        val mapping = context.getMapping(MappingIdentifier("m0"))
        val result = mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input))("main")
        result.count() should be (2)
        result.schema should be (expectedSchema)

        val errors = mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input))("error")
        errors.count() should be (0)
        errors.schema should be (errorSchema)

        val resultSchema = mapping.describe(executor, Map(MappingOutputIdentifier("p0") -> inputSchema))
        resultSchema should be (Map(
            "error" -> com.dimajix.flowman.types.StructType.of(errorSchema),
            "main" -> com.dimajix.flowman.types.StructType(Seq())
        ))

        session.shutdown()
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
        val executor = session.execution
        val context = session.getContext(project)

        val input = executor.spark.createDataFrame(Seq(
            ("""invalid_json""", 12),
            ("""{"st":{"lolo":"x"},"a":[0.1,0.7]}""", 23)
        ))

        val mainSchema = StructType(
            StructField("a", ArrayType(DoubleType, true), true) ::
                StructField("st", StructType(
                    StructField("lolo", StringType, true) ::
                        Nil
                )) ::
                Nil
        )
        val errorSchema = StructType(
            StructField("record", StringType, false) :: Nil
        )

        val mapping = context.getMapping(MappingIdentifier("m0"))
        val result = mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input))("main")
        result.count() should be (1)
        result.schema should be (mainSchema)

        val errors = mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input))("error")
        errors.count() should be (1)
        errors.schema should be (errorSchema)

        session.shutdown()
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
              |      kind: inline
              |      fields:
              |        - name: s
              |          type: String
              |        - name: i
              |          type: Integer
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.getContext(project)

        val input = executor.spark.createDataFrame(Seq(
            ("""invalid_json""", 12),
            ("""{"st":{"lolo":"x"},"a":[0.1,0.7]}""", 23)
        ))

        val mainSchema = StructType(
            StructField("s", StringType, true) ::
            StructField("i", IntegerType, true) ::
            Nil
        )
        val errorSchema = StructType(
            StructField("record", StringType, false) :: Nil
        )

        val mapping = context.getMapping(MappingIdentifier("m0"))
        val result = mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input))("main")
        result.count() should be (1)
        result.schema should be (mainSchema)

        val errors = mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input))("error")
        errors.count() should be (1)
        errors.schema should be (errorSchema)

        session.shutdown()
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
        val executor = session.execution
        val context = session.getContext(project)

        val input = executor.spark.createDataFrame(Seq(
            ("""invalid_json""", 12),
            ("""{"st":{"lolo":"x"},"a":[0.1,0.7]}""", 23)
        ))

        val mainSchema = StructType(
            StructField("a", ArrayType(DoubleType, true), true) ::
                StructField("st", StructType(
                    StructField("lolo", StringType, true) ::
                        Nil
                )) ::
                Nil
        )
        val errorSchema = StructType(
            StructField("record", StringType, false) :: Nil
        )

        val mapping = context.getMapping(MappingIdentifier("m0"))
        val result = mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input))("main")
        result.count() should be (1)
        result.schema should be (mainSchema)

        val errors = mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input))("error")
        errors.count() should be (0)
        errors.schema should be (errorSchema)

        session.shutdown()
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
              |      kind: inline
              |      fields:
              |        - name: s
              |          type: String
              |        - name: i
              |          type: Integer
              |            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.getContext(project)

        val input = executor.spark.createDataFrame(Seq(
            ("""invalid_json""", 12),
            ("""{"st":{"lolo":"x"},"a":[0.1,0.7]}""", 23)
        ))

        val mainSchema = StructType(
            StructField("s", StringType, true) ::
            StructField("i", IntegerType, true) ::
            Nil
        )
        val errorSchema = StructType(
            StructField("record", StringType, false) :: Nil
        )

        val mapping = context.getMapping(MappingIdentifier("m0"))
        val result = mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input))("main")
        result.count() should be (1)
        result.schema should be (mainSchema)

        val errors = mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input))("error")
        errors.count() should be (0)
        errors.schema should be (errorSchema)

        session.shutdown()
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
        val executor = session.execution
        val context = session.getContext(project)

        val input = executor.spark.createDataFrame(Seq(
            ("""invalid_json""", 12),
            ("""{"st":{"lolo":"x"},"a":[0.1,0.7]}""", 23)
        ))

        val mapping = context.getMapping(MappingIdentifier("m0"))
        an[Exception] shouldBe thrownBy(mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input)))

        session.shutdown()
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
              |      kind: inline
              |      fields:
              |        - name: s
              |          type: String
              |        - name: i
              |          type: Integer
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.getContext(project)

        val input = executor.spark.createDataFrame(Seq(
            ("""invalid_json""", 12),
            ("""{"st":{"lolo":"x"},"a":[0.1,0.7]}""", 23)
        ))

        val mapping = context.getMapping(MappingIdentifier("m0"))
        val result = mapping.execute(executor, Map(MappingOutputIdentifier("p0") -> input))("main")
        an[Exception] shouldBe thrownBy(result.count())

        session.shutdown()
    }
}
