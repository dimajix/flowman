/*
 * Copyright 2021 Kaya Kupferschmidt
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
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.spec.schema.EmbeddedSchema
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


class ConstMappingTest extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession {
    "The ConstMapping" should "be parseable with a schema" in {
        val spec =
            """
              |mappings:
              |  fake:
              |    kind: const
              |    records:
              |      - ["a",12,3]
              |      - [cat,"",7]
              |      - [dog,null,8]
              |    schema:
              |      kind: embedded
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().build()
        val context = session.getContext(project)

        val mapping = context.getMapping(MappingIdentifier("fake")).asInstanceOf[ConstMapping]
        mapping shouldBe a[ConstMapping]

        mapping.category should be ("mapping")
        mapping.kind should be ("const")
        mapping.identifier should be (MappingIdentifier("project/fake"))
        mapping.output should be (MappingOutputIdentifier("project/fake:main"))
        mapping.outputs should be (Seq("main"))
        mapping.records.map(_.toSeq) should be (Seq(
            Seq("a","12","3"),
            Seq("cat","","7"),
            Seq("dog",null,"8")
        ))
    }

    it should "be parseable with columns" in {
        val spec =
            """
              |mappings:
              |  fake:
              |    kind: const
              |    records:
              |      - ["a",12,3]
              |      - [cat,"",7]
              |      - [dog,null,8]
              |    columns:
              |      str_col: string
              |      int_col: integer
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().build()
        val context = session.getContext(project)

        val mapping = context.getMapping(MappingIdentifier("fake")).asInstanceOf[ConstMapping]
        mapping shouldBe a[ConstMapping]

        mapping.category should be ("mapping")
        mapping.kind should be ("const")
        mapping.identifier should be (MappingIdentifier("project/fake"))
        mapping.output should be (MappingOutputIdentifier("project/fake:main"))
        mapping.outputs should be (Seq("main"))
        mapping.records.map(_.toSeq) should be (Seq(
            Seq("a","12","3"),
            Seq("cat","","7"),
            Seq("dog",null,"8")
        ))
    }

    it should "work with specified records and schema" in {
        val mappingTemplate = mock[Template[Mapping]]

        val project = Project(
            "my_project",
            mappings = Map(
                "const" -> mappingTemplate
            )
        )
        val schema = new StructType(Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType)
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)
        val executor = session.execution

        val mockMapping = ConstMapping(
            Mapping.Properties(context, "const"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = schema.fields
            )),
            records = Seq(
                Array("lala","12"),
                Array("lolo","13"),
                Array("",null)
            )
        )

        (mappingTemplate.instantiate _).expects(context).returns(mockMapping)
        val mapping = context.getMapping(MappingIdentifier("const"))

        mapping.inputs should be (Seq())
        mapping.outputs should be (Seq("main"))
        mapping.describe(executor, Map()) should be (Map("main" -> schema))
        mapping.describe(executor, Map(), "main") should be (schema)

        val df = executor.instantiate(mapping, "main")
        df.schema should be (schema.sparkType)
        df.collect() should be (Seq(
            Row("lala", 12),
            Row("lolo", 13),
            Row(null,null)
        ))
    }

    it should "work with specified records and columns" in {
        val mappingTemplate = mock[Template[Mapping]]

        val project = Project(
            "my_project",
            mappings = Map(
                "const" -> mappingTemplate
            )
        )
        val schema = new StructType(Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType)
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)
        val executor = session.execution

        val mockMapping = ConstMapping(
            Mapping.Properties(context, "const"),
            columns = schema.fields,
            records = Seq(
                Array("lala","12"),
                Array("lolo","13"),
                Array("",null)
            )
        )

        (mappingTemplate.instantiate _).expects(context).returns(mockMapping)
        val mapping = context.getMapping(MappingIdentifier("const"))

        mapping.inputs should be (Seq())
        mapping.outputs should be (Seq("main"))
        mapping.describe(executor, Map()) should be (Map("main" -> schema))
        mapping.describe(executor, Map(), "main") should be (schema)

        val df = executor.instantiate(mapping, "main")
        df.schema should be (schema.sparkType)
        df.collect() should be (Seq(
            Row("lala", 12),
            Row("lolo", 13),
            Row(null,null)
        ))
    }
}
