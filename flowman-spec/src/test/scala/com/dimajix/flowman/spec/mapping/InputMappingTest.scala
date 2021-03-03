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

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


class InputMappingTest extends FlatSpec with Matchers with LocalSparkSession {
    "The ReadRelationMapping" should "be able to read from a NullRelation" in {
        val spec =
            """
              |relations:
              |  empty:
              |    kind: null
              |mappings:
              |  empty:
              |    kind: read
              |    relation: empty
              |    columns:
              |      str_col: string
              |      int_col: integer
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        project.relations.keys should contain("empty")
        project.mappings.keys should contain("empty")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.getContext(project)

        val mapping = context.getMapping(MappingIdentifier("empty"))
        mapping should not be null

        val df = executor.instantiate(mapping, "main")
        df.columns should contain("str_col")
        df.columns should contain("int_col")

        val schema = mapping.describe(executor, Map(), "main")
        schema should be (StructType(Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType)
        )))
    }

    it should "support embedded schema" in {
        val spec =
            """
              |relations:
              |  empty:
              |    kind: null
              |    schema:
              |      kind: embedded
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
              |mappings:
              |  empty:
              |    kind: read
              |    relation: empty
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        project.relations.keys should contain("empty")
        project.mappings.keys should contain("empty")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.getContext(project)

        val mapping = context.getMapping(MappingIdentifier("empty"))

        val df = executor.instantiate(mapping, "main")
        df.columns should contain("str_col")
        df.columns should contain("int_col")

        val schema = mapping.describe(executor, Map(), "main")
        schema should be (StructType(Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType)
        )))
    }

    it should "support reading from partitions with explicit columns" in {
        val spec =
            """
              |relations:
              |  empty:
              |    kind: null
              |    schema:
              |      kind: embedded
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
              |    partitions:
              |      - name: spart
              |        type: string
              |mappings:
              |  empty:
              |    kind: read
              |    relation: empty
              |    columns:
              |      str_col: string
              |      int_col: integer
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")
        project.relations.keys should contain("empty")
        project.mappings.keys should contain("empty")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.getContext(project)

        val mapping = context.getMapping(MappingIdentifier("empty"))

        val df = executor.instantiate(mapping, "main")
        df.columns should contain("str_col")
        df.columns should contain("int_col")

        val schema = mapping.describe(executor, Map(), "main")
        schema should be (StructType(Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType)
        )))
    }

    it should "support reading from partitions without specification" in {
        val spec =
            """
              |relations:
              |  empty:
              |    kind: null
              |    schema:
              |      kind: embedded
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
              |    partitions:
              |      - name: spart
              |        type: string
              |mappings:
              |  empty:
              |    kind: read
              |    relation: empty
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        project.relations.keys should contain("empty")
        project.mappings.keys should contain("empty")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.getContext(project)

        val mapping = context.getMapping(MappingIdentifier("empty"))

        val df = executor.instantiate(mapping, "main")
        df.columns should contain("str_col")
        df.columns should contain("int_col")
        df.columns should contain("spart")

        val schema = mapping.describe(executor, Map(), "main")
        schema should be (StructType(Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType),
            Field("spart", StringType, false)
        )))
    }

    it should "support reading from partitions with specification" in {
        val spec =
            """
              |relations:
              |  empty:
              |    kind: null
              |    schema:
              |      kind: embedded
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
              |    partitions:
              |      - name: spart
              |        type: string
              |mappings:
              |  empty:
              |    kind: read
              |    relation: empty
              |    partitions:
              |      spart: abc
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        project.relations.keys should contain("empty")
        project.mappings.keys should contain("empty")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.getContext(project)

        val mapping = context.getMapping(MappingIdentifier("empty"))
        mapping should not be null

        val df = executor.instantiate(mapping, "main")
        df.columns should contain("str_col")
        df.columns should contain("int_col")
        df.columns should contain("spart")
    }
}
