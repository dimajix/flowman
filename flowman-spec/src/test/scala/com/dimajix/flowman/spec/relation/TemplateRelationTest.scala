/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.relation

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.RelationIdentifier


class TemplateRelationTest extends AnyFlatSpec with Matchers {
    "A TemplateRelation" should "work" in {
        val spec =
            """
              |relations:
              |  xfs:
              |    kind: values
              |    columns:
              |      _2: string
              |      _1: string
              |
              |  template:
              |    kind: template
              |    relation: xfs
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("template"))
        relation should not be (null)
        relation shouldBe a[TemplateRelation]
        relation.name should be ("template")
    }

    it should "provide own documentation" in {
        val spec =
            """
              |relations:
              |  xfs:
              |    kind: values
              |    columns:
              |      col_1: string
              |
              |  template:
              |    kind: template
              |    relation: xfs
              |    documentation:
              |      description: "This is the template"
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("template"))
        val doc = relation.documentation.get
        doc.relation should be (Some(relation))
        doc.inputs should be (Seq.empty)
        doc.description should be (Some("This is the template"))
    }

    it should "provide templated documentation" in {
        val spec =
            """
              |relations:
              |  xfs:
              |    kind: values
              |    columns:
              |      col_1: string
              |    documentation:
              |      description: "This is the original relation"
              |
              |  template:
              |    kind: template
              |    relation: xfs
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("template"))
        val doc = relation.documentation.get
        doc.relation should be (Some(relation))
        doc.inputs should be (Seq.empty)
        doc.description should be (Some("This is the original relation"))
    }

    it should "provide merged documentation" in {
        val spec =
            """
              |relations:
              |  xfs:
              |    kind: values
              |    columns:
              |      col_1: string
              |    documentation:
              |      description: "This is the original relation"
              |      columns:
              |        - name: col_1
              |          description: "This is col_1"
              |
              |  template:
              |    kind: template
              |    relation: xfs
              |    documentation:
              |      description: "This is the template"
              |      columns:
              |        - name: col_2
              |          description: "This is col_2"
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("template"))
        val doc = relation.documentation.get
        doc.relation should be (Some(relation))
        doc.inputs should be (Seq.empty)
        doc.description should be (Some("This is the template"))
        val outputSchema = doc.schema.get
        outputSchema.columns.length should be (2)
        outputSchema.columns(0).name should be ("col_1")
        outputSchema.columns(0).index should be (-1)
        outputSchema.columns(0).description should be (Some("This is col_1"))
        outputSchema.columns(1).name should be ("col_2")
        outputSchema.columns(1).index should be (-1)
        outputSchema.columns(1).description should be (Some("This is col_2"))
    }
}
