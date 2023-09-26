/*
 * Copyright (C) 2021 The Flowman Authors
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
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.types.ArrayRecord
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


class MockMappingTest extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession {
    "The MockMapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  empty:
              |    kind: empty
              |    columns:
              |      str_col: string
              |      int_col: integer
              |
              |  mock:
              |    kind: mock
              |    mapping: empty
              |    records:
              |      - ["a",12,3]
              |      - [cat,"",7]
              |      - [dog,null,8]
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val mapping = context.getMapping(MappingIdentifier("mock")).asInstanceOf[MockMapping]
        mapping shouldBe a[MockMapping]

        mapping.category should be (Category.MAPPING)
        mapping.kind should be ("mock")
        mapping.mapping should be (MappingIdentifier("empty"))
        mapping.output should be (MappingOutputIdentifier("project/mock:main"))
        mapping.outputs should be (Set("main"))
        mapping.records should be (Seq(
            ArrayRecord("a","12","3"),
            ArrayRecord("cat","","7"),
            ArrayRecord("dog",null,"8")
        ))

        session.shutdown()
    }

    it should "create empty DataFrames" in {
        val baseMappingTemplate = mock[Prototype[Mapping]]
        val baseMapping = mock[Mapping]
        val mockMappingTemplate = mock[Prototype[Mapping]]

        val project = Project(
            "my_project",
            mappings = Map(
                "base" -> baseMappingTemplate,
                "mock" -> mockMappingTemplate
            )
        )
        val otherSchema = new StructType(Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType)
        ))
        val errorSchema = new StructType(Seq(
            Field("error", StringType)
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)
        val executor = session.execution

        val mockMapping = MockMapping(
            Mapping.Properties(context, "mock"),
            MappingIdentifier("base")
        )

        (mockMappingTemplate.instantiate _).expects(context, None).returns(mockMapping)
        val mapping = context.getMapping(MappingIdentifier("mock"))
        mapping shouldBe a[MockMapping]
        mapping.category should be (Category.MAPPING)

        (baseMappingTemplate.instantiate _).expects(context, None).returns(baseMapping)
        (baseMapping.outputs _).expects().anyNumberOfTimes().returns(Set("other", "error"))
        mapping.outputs should be (Set("other", "error"))

        (baseMapping.output _).expects().returns(MappingOutputIdentifier("base", "other", Some(project.name)))
        mapping.output should be (MappingOutputIdentifier("my_project/mock:other"))

        (baseMapping.context _).expects().anyNumberOfTimes().returns(context)
        (baseMapping.inputs _).expects().anyNumberOfTimes().returns(Set())
        (baseMapping.identifier _).expects().anyNumberOfTimes().returns(MappingIdentifier("my_project/base"))
        (baseMapping.describe:(Execution,Map[MappingOutputIdentifier,StructType]) => Map[String,StructType]).expects(executor,*)
            .anyNumberOfTimes().returns(Map("other" -> otherSchema, "error" -> errorSchema))
        mapping.describe(executor, Map()) should be (Map(
            "other" -> otherSchema,
            "error" -> errorSchema
        ))

        mapping.describe(executor, Map())("other") should be (otherSchema)

        val dfOther = executor.instantiate(mapping, "other")
        dfOther.columns should contain("str_col")
        dfOther.columns should contain("int_col")
        dfOther.count() should be (0)

        val dfError = executor.instantiate(mapping, "error")
        dfError.columns should contain("error")
        dfError.count() should be (0)

        session.shutdown()
    }

    it should "work nicely as an override" in {
        val baseMappingTemplate = mock[Prototype[Mapping]]
        val baseMapping = mock[Mapping]
        val mockMappingTemplate = mock[Prototype[Mapping]]

        val project = Project(
            "my_project",
            mappings = Map(
                "mock" -> baseMappingTemplate
            )
        )
        val schema = new StructType(Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType)
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val rootContext = RootContext.builder(session.context)
            .overrideMappings(Map(
                MappingIdentifier("mock", "my_project") -> mockMappingTemplate
            ))
            .build()
        val context = rootContext.getProjectContext(project)
        val executor = session.execution

        val mockMapping = MockMapping(
            Mapping.Properties(context, "mock"),
            MappingIdentifier("mock")
        )

        (mockMappingTemplate.instantiate _).expects(context, None).returns(mockMapping)
        val mapping = context.getMapping(MappingIdentifier("mock"))

        (baseMappingTemplate.instantiate _).expects(context, None).returns(baseMapping)
        (baseMapping.outputs _).expects().anyNumberOfTimes().returns(Set("main"))
        mapping.outputs should be (Set("main"))

        (baseMapping.output _).expects().returns(MappingOutputIdentifier("mock", "main", Some(project.name)))
        mapping.output should be (MappingOutputIdentifier("my_project/mock:main"))

        (baseMapping.context _).expects().anyNumberOfTimes().returns(context)
        (baseMapping.inputs _).expects().anyNumberOfTimes().returns(Set())
        (baseMapping.identifier _).expects().anyNumberOfTimes().returns(MappingIdentifier("my_project/base"))
        (baseMapping.describe:(Execution,Map[MappingOutputIdentifier,StructType]) => Map[String,StructType]).expects(executor,*)
            .anyNumberOfTimes().returns(Map("main" -> schema))
        mapping.describe(executor, Map()) should be (Map("main" -> schema))

        val dfOther = executor.instantiate(mapping, "main")
        dfOther.columns should contain("str_col")
        dfOther.columns should contain("int_col")
        dfOther.count() should be (0)

        session.shutdown()
    }

    it should "work with specified records" in {
        val baseMappingTemplate = mock[Prototype[Mapping]]
        val baseMapping = mock[Mapping]
        val mockMappingTemplate = mock[Prototype[Mapping]]

        val project = Project(
            "my_project",
            mappings = Map(
                "base" -> baseMappingTemplate,
                "mock" -> mockMappingTemplate
            )
        )
        val schema = new StructType(Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType)
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)
        val executor = session.execution

        val mockMapping = MockMapping(
            Mapping.Properties(context, "mock"),
            MappingIdentifier("base"),
            Seq(
                ArrayRecord("lala","12"),
                ArrayRecord("lolo","13"),
                ArrayRecord("",""),
                ArrayRecord(null,null)
            )
        )

        (mockMappingTemplate.instantiate _).expects(context, None).returns(mockMapping)
        val mapping = context.getMapping(MappingIdentifier("mock"))

        (baseMappingTemplate.instantiate _).expects(context, None).returns(baseMapping)
        (baseMapping.context _).expects().anyNumberOfTimes().returns(context)
        (baseMapping.outputs _).expects().anyNumberOfTimes().returns(Set("main"))
        (baseMapping.inputs _).expects().anyNumberOfTimes().returns(Set())
        (baseMapping.identifier _).expects().anyNumberOfTimes().returns(MappingIdentifier("my_project/base"))
        (baseMapping.describe:(Execution,Map[MappingOutputIdentifier,StructType]) => Map[String,StructType]).expects(executor,*)
            .anyNumberOfTimes().returns(Map("main" -> schema))

        val df = executor.instantiate(mapping, "main")
        df.schema should be (schema.sparkType)
        df.collect() should be (Seq(
            Row("lala", 12),
            Row("lolo", 13),
            Row("",null),
            Row(null,null)
        ))

        session.shutdown()
    }
}
