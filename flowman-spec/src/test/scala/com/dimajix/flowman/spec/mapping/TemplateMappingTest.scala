/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.documentation.MappingOutputDoc
import com.dimajix.flowman.documentation.MappingOutputReference
import com.dimajix.flowman.documentation.MappingReference
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module


class TemplateMappingTest extends AnyFlatSpec with Matchers {
    "A TemplateMapping" should "work" in {
        val spec =
            """
              |mappings:
              |  xfs:
              |    kind: schema
              |    input: ${input}
              |    columns:
              |      _2: string
              |      _1: string
              |
              |  template:
              |    kind: template
              |    mapping: xfs
              |    environment:
              |      - input=$default_input
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder()
            .disableSpark()
            .withEnvironment("default_input", "lala")
            .build()
        val context = session.getContext(project)

        val mapping = context.getMapping(MappingIdentifier("template"))
        mapping should not be (null)
        mapping shouldBe a[TemplateMapping]
        mapping.name should be ("template")
        mapping.kind should be ("template")
        mapping.inputs should be (Set(MappingOutputIdentifier("lala")))
        mapping.identifier should be (MappingIdentifier("project/template"))
        mapping.project should be (Some(project))

        val instance = mapping.asInstanceOf[TemplateMapping].mappingInstance
        instance shouldBe a[SchemaMapping]
        instance.name should be ("template")
        instance.kind should be ("schema")
        instance.identifier should be (MappingIdentifier("project/template"))
        instance.project should be (Some(project))

        session.shutdown()
    }

    it should "provide own documentation" in {
        val spec =
            """
              |mappings:
              |  xfs:
              |    kind: schema
              |    input: input
              |    columns:
              |      col_1: string
              |
              |  template:
              |    kind: template
              |    mapping: xfs
              |    documentation:
              |      description: "This is the template"
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val mapping = context.getMapping(MappingIdentifier("template"))
        val doc = mapping.documentation.get
        doc.mapping should be (Some(mapping))
        doc.inputs should be (Seq.empty)
        doc.description should be (Some("This is the template"))
        doc.outputs should be (Seq.empty)

        session.shutdown()
    }

    it should "provide templated documentation" in {
        val spec =
            """
              |mappings:
              |  xfs:
              |    kind: schema
              |    input: input
              |    columns:
              |      col_1: string
              |    documentation:
              |      description: "This is the original mapping"
              |
              |  template:
              |    kind: template
              |    mapping: xfs
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val mapping = context.getMapping(MappingIdentifier("template"))
        val doc = mapping.documentation.get
        doc.mapping should be (Some(mapping))
        doc.inputs should be (Seq.empty)
        doc.description should be (Some("This is the original mapping"))
        doc.outputs should be (Seq.empty)

        session.shutdown()
    }

    it should "provide merged documentation" in {
        val spec =
            """
              |mappings:
              |  xfs:
              |    kind: schema
              |    input: input
              |    columns:
              |      col_1: string
              |    documentation:
              |      description: "This is the original mapping"
              |      columns:
              |        - name: col_1
              |          description: "This is col_1"
              |
              |  template:
              |    kind: template
              |    mapping: xfs
              |    documentation:
              |      description: "This is the template"
              |      columns:
              |        - name: col_2
              |          description: "This is col_2"
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val mapping = context.getMapping(MappingIdentifier("template"))
        val doc = mapping.documentation.get
        doc.mapping should be (Some(mapping))
        doc.inputs should be (Seq.empty)
        doc.description should be (Some("This is the template"))
        val output = doc.outputs.head
        val outputSchema = output.schema.get
        outputSchema.columns.length should be (2)
        outputSchema.columns(0).name should be ("col_1")
        outputSchema.columns(0).index should be (-1)
        outputSchema.columns(0).description should be (Some("This is col_1"))
        outputSchema.columns(1).name should be ("col_2")
        outputSchema.columns(1).index should be (-1)
        outputSchema.columns(1).description should be (Some("This is col_2"))

        session.shutdown()
    }
}
