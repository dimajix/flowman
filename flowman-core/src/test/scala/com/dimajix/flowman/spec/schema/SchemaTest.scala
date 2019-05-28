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

package com.dimajix.flowman.spec.schema

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session


class SchemaTest extends FlatSpec with Matchers {
    lazy val mapper = {
        val mapper = new ObjectMapper(new YAMLFactory())
        mapper.registerModule(DefaultScalaModule)
        mapper
    }

    "A Schema" should "default to the embedded schema" in {
        val spec =
            """
              |fields:
              |  - name: str_col
              |    type: string
              |  - name: int_col
              |    type: integer
            """.stripMargin

        val result = mapper.readValue(spec, classOf[SchemaSpec])
        result shouldBe a[EmbeddedSchemaSpec]
    }

    it should "provide a nice string representation (1)" in {
        val spec =
            """
              |fields:
              |  - name: str_col
              |    type: string
              |  - name: int_col
              |    type: integer
            """.stripMargin

        val session = Session.builder().build()
        val result = mapper.readValue(spec, classOf[SchemaSpec])

        val schema = result.instantiate(session.context)
        schema.treeString should be (
            """root
              | |-- str_col: string (nullable = true)
              | |-- int_col: integer (nullable = true)
              |""".stripMargin)

    }

    it should "provide a nice string representation (2)" in {
        val spec =
            """
              |fields:
              |  - name: str_col
              |    type: string
              |  - name: array_col
              |    type:
              |      kind: array
              |      elementType: String
              |""".stripMargin

        val session = Session.builder().build()
        val result = mapper.readValue(spec, classOf[SchemaSpec])

        val schema = result.instantiate(session.context)
        schema.treeString should be (
            """root
              | |-- str_col: string (nullable = true)
              | |-- array_col: array (nullable = true)
              | |    |-- element: string (containsNull = true)
              |""".stripMargin)

    }

    it should "provide a nice string representation (3)" in {
        val spec =
            """
              |fields:
              |  - name: str_col
              |    type: string
              |  - name: struct_col
              |    type:
              |      kind: struct
              |      fields:
              |        - name: lolo
              |          type: String
              |          nullable: false
              |        - name: lili
              |          type: int
              |          nullable: true
              |""".stripMargin

        val session = Session.builder().build()
        val result = mapper.readValue(spec, classOf[SchemaSpec])

        val schema = result.instantiate(session.context)
        schema.treeString should be (
            """root
              | |-- str_col: string (nullable = true)
              | |-- struct_col: struct (nullable = true)
              | |    |-- lolo: string (nullable = false)
              | |    |-- lili: integer (nullable = true)
              |""".stripMargin)
    }

    it should "provide a nice string representation (4)" in {
        val spec =
            """
              |fields:
              |  - name: str_col
              |    type: string
              |  - name: struct_col
              |    type:
              |      kind: struct
              |      fields:
              |        - name: lolo
              |          type: String
              |          nullable: false
              |        - name: array_col
              |          type:
              |            kind: array
              |            elementType: varchar(5)
              |        - name: lili
              |          type: int
              |          nullable: true
              |""".stripMargin

        val session = Session.builder().build()
        val result = mapper.readValue(spec, classOf[SchemaSpec])

        val schema = result.instantiate(session.context)
        schema.treeString should be (
            """root
              | |-- str_col: string (nullable = true)
              | |-- struct_col: struct (nullable = true)
              | |    |-- lolo: string (nullable = false)
              | |    |-- array_col: array (nullable = true)
              | |    |    |-- element: varchar(5) (containsNull = true)
              | |    |-- lili: integer (nullable = true)
              |""".stripMargin)
    }
}
