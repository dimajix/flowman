/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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
import com.dimajix.flowman.model.Category
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.spec.schema.InlineSchema
import com.dimajix.flowman.types.ArrayRecord
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


class ValuesMappingTest extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession {
    "The ValuesMapping" should "be parseable with a schema" in {
        val spec =
            """
              |mappings:
              |  fake:
              |    kind: values
              |    records:
              |      - ["a",12,3]
              |      - [cat,"",7]
              |      - [dog,null,8]
              |    schema:
              |      kind: inline
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val mapping = context.getMapping(MappingIdentifier("fake")).asInstanceOf[ValuesMapping]
        mapping shouldBe a[ValuesMapping]

        mapping.category should be (Category.MAPPING)
        mapping.kind should be ("values")
        mapping.identifier should be (MappingIdentifier("project/fake"))
        mapping.output should be (MappingOutputIdentifier("project/fake:main"))
        mapping.outputs should be (Set("main"))
        mapping.records should be (Seq(
            ArrayRecord("a","12","3"),
            ArrayRecord("cat","","7"),
            ArrayRecord("dog",null,"8")
        ))

        session.shutdown()
    }

    it should "be parseable with columns" in {
        val spec =
            """
              |mappings:
              |  fake:
              |    kind: values
              |    records:
              |      - ["a",12,3]
              |      - [cat,"",7]
              |      - [dog,null,8]
              |    columns:
              |      str_col: string
              |      int_col: integer
              |      some_col: string
              |      other_col: string
              |      last_col: string
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val mapping = context.getMapping(MappingIdentifier("fake")).asInstanceOf[ValuesMapping]
        mapping shouldBe a[ValuesMapping]

        mapping.category should be (Category.MAPPING)
        mapping.kind should be ("values")
        mapping.identifier should be (MappingIdentifier("project/fake"))
        mapping.inputs should be (Set())
        mapping.output should be (MappingOutputIdentifier("project/fake:main"))
        mapping.outputs should be (Set("main"))
        mapping.columns should be (Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType),
            Field("some_col", StringType),
            Field("other_col", StringType),
            Field("last_col", StringType)
        ))
        mapping.records should be (Seq(
            ArrayRecord("a","12","3"),
            ArrayRecord("cat","","7"),
            ArrayRecord("dog",null,"8")
        ))

        session.shutdown()
    }

    it should "work with specified records and schema" in {
        val mappingTemplate = mock[Prototype[Mapping]]

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

        val mockMapping = ValuesMapping(
            Mapping.Properties(context, "const"),
            schema = Some(InlineSchema(
                Schema.Properties(context),
                fields = schema.fields
            )),
            records = Seq(
                ArrayRecord("lala","12"),
                ArrayRecord("lolo","13"),
                ArrayRecord("",null)
            )
        )

        (mappingTemplate.instantiate _).expects(context, None).returns(mockMapping)
        val mapping = context.getMapping(MappingIdentifier("const"))

        mapping.inputs should be (Set())
        mapping.outputs should be (Set("main"))
        mapping.describe(executor, Map()) should be (Map("main" -> schema))

        val df = executor.instantiate(mapping, "main")
        df.schema should be (schema.sparkType)
        df.collect() should be (Seq(
            Row("lala", 12),
            Row("lolo", 13),
            Row(null,null)
        ))

        session.shutdown()
    }

    it should "work with specified records and columns" in {
        val mappingTemplate = mock[Prototype[Mapping]]

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

        val mockMapping = ValuesMapping(
            Mapping.Properties(context, "const"),
            columns = schema.fields,
            records = Seq(
                ArrayRecord("lala","12"),
                ArrayRecord("lolo","13"),
                ArrayRecord("",null)
            )
        )

        (mappingTemplate.instantiate _).expects(context, None).returns(mockMapping)
        val mapping = context.getMapping(MappingIdentifier("const"))

        mapping.inputs should be (Set())
        mapping.outputs should be (Set("main"))
        mapping.describe(executor, Map()) should be (Map("main" -> schema))

        val df = executor.instantiate(mapping, "main")
        df.schema should be (schema.sparkType)
        df.collect() should be (Seq(
            Row("lala", 12),
            Row("lolo", 13),
            Row(null,null)
        ))

        session.shutdown()
    }
}
