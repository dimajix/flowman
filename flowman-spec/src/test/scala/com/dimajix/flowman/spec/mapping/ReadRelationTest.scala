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

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.graph.Graph
import com.dimajix.flowman.model.IdentifierRelationReference
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ValueRelationReference
import com.dimajix.flowman.spec.relation.ValuesRelation
import com.dimajix.flowman.types.SingleValue


class ReadRelationTest extends AnyFlatSpec with Matchers {
    "A ReadRelationMapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  t0:
              |    kind: readRelation
              |    relation: some_relation
              |    filter: "landing_date > 123"
              |    partitions:
              |      p0: "12"
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withProject(project).disableSpark().build()
        val context = session.getContext(project)
        val mapping = context.getMapping(MappingIdentifier("t0"))

        mapping shouldBe a[ReadRelationMapping]
        val rrm = mapping.asInstanceOf[ReadRelationMapping]
        rrm.relation should be (IdentifierRelationReference(context, RelationIdentifier("some_relation")))
        rrm.filter should be (Some("landing_date > 123"))
        rrm.partitions should be (Map("p0" -> SingleValue("12")))
    }

    it should "support embedded relations" in {
        val spec =
            """
              |mappings:
              |  t0:
              |    kind: readRelation
              |    relation:
              |      name: embedded
              |      kind: values
              |      records:
              |        - ["key",12]
              |      schema:
              |        kind: embedded
              |        fields:
              |          - name: key_column
              |            type: string
              |          - name: value_column
              |            type: integer
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withProject(project).disableSpark().build()
        val context = session.getContext(project)
        val mapping = context.getMapping(MappingIdentifier("t0"))

        mapping shouldBe a[ReadRelationMapping]
        val rrm = mapping.asInstanceOf[ReadRelationMapping]
        rrm.relation shouldBe a[ValueRelationReference]
        rrm.relation.identifier should be (RelationIdentifier("embedded", "project"))
        rrm.relation.name should be ("embedded")

        // Check execution graph
        val graph = Graph.ofProject(context, project, Phase.BUILD)
        graph.relations.size should be (1)
        graph.mappings.size should be (1)

        val relNode = graph.relations.head
        relNode.relation should be (rrm.relation.value)
        relNode.parent should be (None)

        val mapNode = graph.mappings.head
        mapNode.mapping should be (rrm)
        mapNode.parent should be (None)
    }

    it should "support create an appropriate graph" in {
        val spec =
            """
              |mappings:
              |  t0:
              |    kind: readRelation
              |    relation:
              |      name: embedded
              |      kind: values
              |      records:
              |        - ["key",12]
              |      schema:
              |        kind: embedded
              |        fields:
              |          - name: key_column
              |            type: string
              |          - name: value_column
              |            type: integer
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withProject(project).disableSpark().build()
        val context = session.getContext(project)
        val mapping = context.getMapping(MappingIdentifier("t0"))

        mapping shouldBe a[ReadRelationMapping]
        val relation = mapping.asInstanceOf[ReadRelationMapping].relation.value
        relation shouldBe a[ValuesRelation]

        // Check execution graph
        val graph = Graph.ofProject(context, project, Phase.BUILD)
        graph.nodes.size should be (3) // 1 mapping + 1 mapping output + 1 relation
        graph.relations.size should be (1)
        graph.mappings.size should be (1)

        val relNode = graph.relations.head
        relNode.relation should be (relation)
        relNode.parent should be (None)

        val mapNode = graph.mappings.head
        mapNode.mapping should be (mapping)
        mapNode.parent should be (None)
    }
}
