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

package com.dimajix.flowman.graph

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StructType


class GraphTest extends AnyFlatSpec with Matchers with MockFactory {
    "Graph.ofProject" should "work" in {
        val mapping1 = mock[Mapping]
        val mappingTemplate1 = mock[Prototype[Mapping]]
        val mapping2 = mock[Mapping]
        val mappingTemplate2 = mock[Prototype[Mapping]]
        val sourceRelation = mock[Relation]
        val sourceRelationTemplate = mock[Prototype[Relation]]
        val targetRelation = mock[Relation]
        val targetRelationTemplate = mock[Prototype[Relation]]
        val target = mock[Target]
        val targetTemplate = mock[Prototype[Target]]

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
        val session = Session.builder().withProject(project).disableSpark().build()
        val context = session.getContext(project)

        (mappingTemplate1.instantiate _).expects(context,None).returns(mapping1)
        (mapping1.identifier _).expects().returns(MappingIdentifier("project/m1"))
        (mapping1.context _).expects().atLeastOnce().returns(context)
        (mapping1.outputs _).expects().atLeastOnce().returns(Set("main"))
        (mapping1.inputs _).expects().returns(Set(MappingOutputIdentifier("m2")))
        (mapping1.name _).expects().atLeastOnce().returns("m1")
        (mapping1.describe _).expects(*,*).atLeastOnce().returns(Map("main" -> StructType(Seq.empty)))
        (mapping1.link _).expects(*).onCall((l:Linker) => Some(1).foreach(_ => l.input(MappingIdentifier("m2"), "main")))

        (mappingTemplate2.instantiate _).expects(context,None).returns(mapping2)
        (mapping2.identifier _).expects().atLeastOnce().returns(MappingIdentifier("project/m2"))
        (mapping2.context _).expects().atLeastOnce().returns(context)
        (mapping2.outputs _).expects().atLeastOnce().returns(Set("main"))
        (mapping2.inputs _).expects().returns(Set())
        (mapping2.name _).expects().atLeastOnce().returns("m2")
        (mapping2.describe _).expects(*,*).atLeastOnce().returns(Map("main" -> StructType(Seq.empty)))
        (mapping2.link _).expects(*).onCall((l:Linker) => Some(1).foreach(_ => l.read(RelationIdentifier("src"), Map.empty[String,FieldValue])))

        (sourceRelationTemplate.instantiate _).expects(context,None).returns(sourceRelation)
        (sourceRelation.identifier _).expects().atLeastOnce().returns(RelationIdentifier("project/src"))
        (sourceRelation.context _).expects().returns(context)
        (sourceRelation.name _).expects().atLeastOnce().returns("src")
        (sourceRelation.describe _).expects(*,*).atLeastOnce().returns(StructType(Seq.empty))
        (sourceRelation.link _).expects(*).returns(Unit)

        (targetRelationTemplate.instantiate _).expects(context,None).returns(targetRelation)
        (targetRelation.identifier _).expects().atLeastOnce().returns(RelationIdentifier("project/tgt"))
        (targetRelation.context _).expects().returns(context)
        (targetRelation.name _).expects().atLeastOnce().returns("tgt")
        (targetRelation.describe _).expects(*,*).atLeastOnce().returns(StructType(Seq.empty))
        (targetRelation.link _).expects(*).returns(Unit)

        (targetTemplate.instantiate _).expects(context,None).returns(target)
        (target.context _).expects().returns(context)
        (target.name _).expects().atLeastOnce().returns("t")
        (target.link _).expects(*,*).onCall((l:Linker, _:Phase) => Some(1).foreach { _ =>
            l.input(MappingIdentifier("m1"), "main")
            l.write(RelationIdentifier("tgt"), Map.empty[String,SingleValue])
        })

        val graph = Graph.ofProject(session, project, Phase.BUILD)

        val nodes = graph.nodes
        nodes.size should be (7)
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
        val out1main = m1.outputs.head
        val out2main = m2.outputs.head

        val tgts = graph.targets
        tgts.size should be (1)
        val t = tgts.head

        val rels = graph.relations
        rels.size should be (2)
        val src = rels.find(_.name == "src").get
        val tgt = rels.find(_.name == "tgt").get

        m1.incoming should be (Seq(InputMapping(out2main, m1)))
        m1.outgoing should be (Seq())
        m1.outputs.head.outgoing should be (Seq(InputMapping(out1main, t)))
        m2.incoming should be (Seq(ReadRelation(src, m2, Map())))
        m2.outgoing should be (Seq())
        m2.outputs.head.outgoing should be (Seq(InputMapping(out2main, m1)))
        src.incoming should be (Seq())
        src.outgoing should be (Seq(ReadRelation(src, m2, Map())))
        t.incoming should be (Seq(InputMapping(out1main, t)))
        t.outgoing should be (Seq(WriteRelation(t, tgt, Map())))
        tgt.incoming should be (Seq(WriteRelation(t, tgt, Map())))
        tgt.outgoing should be (Seq())

        graph.relation(RelationIdentifier("src")) should be (src)
        graph.relation(sourceRelation) should be (src)
        graph.mapping(MappingIdentifier("m1")) should be (m1)
        graph.mapping(mapping1) should be (m1)
        graph.target(TargetIdentifier("t")) should be (t)
        graph.target(target) should be (t)

        session.shutdown()
    }
}
