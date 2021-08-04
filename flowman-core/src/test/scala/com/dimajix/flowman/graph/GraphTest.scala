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

package com.dimajix.flowman.graph

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.Template


class GraphTest extends AnyFlatSpec with Matchers with MockFactory {
    "Graph.ofProject" should "work" in {
        val mapping1 = mock[Mapping]
        val mappingTemplate1 = mock[Template[Mapping]]
        val mapping2 = mock[Mapping]
        val mappingTemplate2 = mock[Template[Mapping]]
        val sourceRelation = mock[Relation]
        val sourceRelationTemplate = mock[Template[Relation]]
        val targetRelation = mock[Relation]
        val targetRelationTemplate = mock[Template[Relation]]
        val target = mock[Target]
        val targetTemplate = mock[Template[Target]]

        val project = Project(
            name = "project",
            mappings = Map(
                "m1" -> mappingTemplate1,
                "m2" -> mappingTemplate2
            ),
            targets = Map(
                "t" -> targetTemplate
            ),
            relations = Map(
                "src" -> sourceRelationTemplate,
                "tgt" -> targetRelationTemplate
            )
        )
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        (mappingTemplate1.instantiate _).expects(context).returns(mapping1)
        (mapping1.context _).expects().returns(context)
        (mapping1.name _).expects().atLeastOnce().returns("m1")
        (mapping1.link _).expects(*).onCall((l:Linker) => Some(1).foreach(_ => l.input(MappingIdentifier("m2"), "main")))

        (mappingTemplate2.instantiate _).expects(context).returns(mapping2)
        (mapping2.context _).expects().returns(context)
        (mapping2.name _).expects().atLeastOnce().returns("m2")
        (mapping2.link _).expects(*).onCall((l:Linker) => Some(1).foreach(_ => l.read(RelationIdentifier("src"), Map())))

        (sourceRelationTemplate.instantiate _).expects(context).returns(sourceRelation)
        (sourceRelation.context _).expects().returns(context)
        (sourceRelation.name _).expects().atLeastOnce().returns("src")
        (sourceRelation.link _).expects(*).returns(Unit)

        (targetRelationTemplate.instantiate _).expects(context).returns(targetRelation)
        (targetRelation.context _).expects().returns(context)
        (targetRelation.name _).expects().atLeastOnce().returns("tgt")
        (targetRelation.link _).expects(*).returns(Unit)

        (targetTemplate.instantiate _).expects(context).returns(target)
        (target.context _).expects().returns(context)
        (target.name _).expects().atLeastOnce().returns("t")
        (target.link _).expects(*).onCall((l:Linker) => Some(1).foreach { _ =>
            l.input(MappingIdentifier("m1"), "main")
            l.write(RelationIdentifier("tgt"), Map())
        })

        val graph = Graph.ofProject(session, project)

        val nodes = graph.nodes
        nodes.size should be (5)
        nodes.find(_.name == "m1") should not be (None)
        nodes.find(_.name == "m1").get shouldBe a[MappingRef]
        nodes.find(_.name == "m2") should not be (None)
        nodes.find(_.name == "m2").get shouldBe a[MappingRef]
        nodes.find(_.name == "m3") should be (None)
        nodes.find(_.name == "src") should not be (None)
        nodes.find(_.name == "src").get shouldBe a[RelationRef]
        nodes.find(_.name == "tgt") should not be (None)
        nodes.find(_.name == "tgt").get shouldBe a[RelationRef]
        nodes.find(_.name == "t") should not be (None)
        nodes.find(_.name == "t").get shouldBe a[TargetRef]

        val edges = graph.edges
        edges.size should be (4)

        val maps = graph.mappings
        maps.size should be (2)
        maps.find(_.name == "m1") should not be (None)
        maps.find(_.name == "m2") should not be (None)
        maps.find(_.name == "m3") should be (None)
        val m1 = maps.find(_.name == "m1").get
        val m2 = maps.find(_.name == "m2").get

        val tgts = graph.targets
        tgts.size should be (1)
        val t = tgts.head

        val rels = graph.relations
        rels.size should be (2)
        val src = rels.find(_.name == "src").get
        val tgt = rels.find(_.name == "tgt").get

        m1.incoming should be (Seq(InputMapping(m2, m1, "main")))
        m1.outgoing should be (Seq(InputMapping(m1, t, "main")))
        m2.incoming should be (Seq(ReadRelation(src, m2, Map())))
        m2.outgoing should be (Seq(InputMapping(m2, m1, "main")))
        src.incoming should be (Seq())
        src.outgoing should be (Seq(ReadRelation(src, m2, Map())))
        t.incoming should be (Seq(InputMapping(m1, t, "main")))
        t.outgoing should be (Seq(WriteRelation(t, tgt, Map())))
        tgt.incoming should be (Seq(WriteRelation(t, tgt, Map())))
        tgt.outgoing should be (Seq())

        graph.relation(RelationIdentifier("src")) should be (src)
        graph.relation(sourceRelation) should be (src)
        graph.mapping(MappingIdentifier("m1")) should be (m1)
        graph.mapping(mapping1) should be (m1)
        graph.target(TargetIdentifier("t")) should be (t)
        graph.target(target) should be (t)
    }
}
