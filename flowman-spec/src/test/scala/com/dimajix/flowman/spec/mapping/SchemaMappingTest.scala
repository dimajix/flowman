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

package com.dimajix.flowman.spec.mapping

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.spark.testing.LocalSparkSession


class SchemaMappingTest extends FlatSpec with Matchers with LocalSparkSession {
    "The SchemaMapping" should "work" in {
        val df = spark.createDataFrame(Seq(
            ("col1", 12),
            ("col2", 23)
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val mapping = SchemaMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("myview"),
            Seq("_2" -> "int")
        )

        mapping.input should be (MappingOutputIdentifier("myview"))
        mapping.columns should be (Seq("_2" -> "int"))
        mapping.inputs should be (Seq(MappingOutputIdentifier("myview")))

        val result = mapping.execute(executor, Map(MappingOutputIdentifier("myview") -> df))("main")
            .orderBy("_2").collect()
        result.size should be (2)
        result(0) should be (Row(12))
        result(1) should be (Row(23))
    }

    it should "add NULL columns for missing columns" in {
        val df = spark.createDataFrame(Seq(
            ("col1", 12),
            ("col2", 23)
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val mapping = SchemaMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("myview"),
            Seq("_2" -> "int", "new" -> "string")
        )

        mapping.input should be (MappingOutputIdentifier("myview"))
        mapping.columns should be (Seq("_2" -> "int", "new" -> "string"))
        mapping.inputs should be (Seq(MappingOutputIdentifier("myview")))

        val result = mapping.execute(executor, Map(MappingOutputIdentifier("myview") -> df))("main")
            .orderBy("_2")
        result.schema should be (StructType(Seq(
            StructField("_2", IntegerType, false),
            StructField("new", StringType, true)
        )))
        val rows = result.collect()
        rows.size should be (2)
        rows(0) should be (Row(12, null))
        rows(1) should be (Row(23, null))
    }

    "An appropriate Dataflow" should "be readable from YML" in {
        val spec =
            """
              |mappings:
              |  t1:
              |    kind: schema
              |    input: t0
              |    columns:
              |      _2: string
              |      _1: string
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.getContext(project)

        project.mappings.size should be (1)
        project.mappings.contains("t0") should be (false)
        project.mappings.contains("t1") should be (true)

        val df = spark.createDataFrame(Seq(
            ("col1", 12),
            ("col2", 23)
        ))

        val mapping = context.getMapping(MappingIdentifier("t1"))
        mapping.execute(executor, Map(MappingOutputIdentifier("t0") -> df))
    }

}
