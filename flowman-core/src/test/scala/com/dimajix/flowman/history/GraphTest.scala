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

package com.dimajix.flowman.history

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.graph.Category
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.{graph => g}


class GraphTest extends AnyFlatSpec with Matchers with MockFactory {
  "Graph.ofGraph" should "work" in {
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
    val session = Session.builder().disableSpark().build()
    val context = session.getContext(project)

    (mappingTemplate1.instantiate _).expects(context, None).returns(mapping1)
    (mapping1.identifier _).expects().returns(MappingIdentifier("project/m1"))
    (mapping1.context _).expects().atLeastOnce().returns(context)
    (mapping1.inputs _).expects().returns(Set(MappingOutputIdentifier("m2")))
    (mapping1.outputs _).expects().atLeastOnce().returns(Set("main"))
    (mapping1.link _).expects(*).onCall((l: Linker) => Some(1).foreach(_ => l.input(MappingIdentifier("m2"), "main")))
    (mapping1.describe _).expects(*,*).returns(Map("main" -> StructType(Seq.empty)))

    (mappingTemplate2.instantiate _).expects(context, None).returns(mapping2)
    (mapping2.identifier _).expects().atLeastOnce().returns(MappingIdentifier("project/m2"))
    (mapping2.context _).expects().atLeastOnce().returns(context)
    (mapping2.inputs _).expects().returns(Set())
    (mapping2.outputs _).expects().atLeastOnce().returns(Set("main"))
    (mapping2.link _).expects(*).onCall((l: Linker) => Some(1).foreach(_ => l.read(RelationIdentifier("src"), Map("pcol" -> SingleValue("part1")))))
    (mapping2.describe _).expects(*,*).returns(Map("main" -> StructType(Seq.empty)))

    (sourceRelationTemplate.instantiate _).expects(context, None).returns(sourceRelation)
    (sourceRelation.identifier _).expects().atLeastOnce().returns(RelationIdentifier("project/src"))
    (sourceRelation.context _).expects().returns(context)
    (sourceRelation.link _).expects(*).returns(Unit)
    (sourceRelation.describe _).expects(*,*).returns(StructType(Seq.empty))

    (targetRelationTemplate.instantiate _).expects(context, None).returns(targetRelation)
    (targetRelation.identifier _).expects().atLeastOnce().returns(RelationIdentifier("project/tgt"))
    (targetRelation.context _).expects().returns(context)
    (targetRelation.link _).expects(*).returns(Unit)
    (targetRelation.describe _).expects(*,*).returns(StructType(Seq.empty))

    (targetTemplate.instantiate _).expects(context, None).returns(target)
    (target.context _).expects().returns(context)
    (target.link _).expects(*, *).onCall((l: Linker, _: Phase) => Some(1).foreach { _ =>
      l.input(MappingIdentifier("m1"), "main")
      l.write(RelationIdentifier("tgt"), Map("outcol" -> SingleValue("part1")))
    })

    val graph = g.Graph.ofProject(session, project, Phase.BUILD)

    (mapping1.name _).expects().atLeastOnce().returns("m1")
    (mapping1.kind _).expects().atLeastOnce().returns("m1_kind")
    (mapping1.requires _).expects().returns(Set())
    (mapping2.name _).expects().atLeastOnce().returns("m2")
    (mapping2.kind _).expects().atLeastOnce().returns("m2_kind")
    (mapping2.requires _).expects().returns(Set())

    (sourceRelation.name _).expects().atLeastOnce().returns("src")
    (sourceRelation.kind _).expects().atLeastOnce().returns("src_kind")
    (sourceRelation.provides _).expects(*,*).returns(Set())
    (sourceRelation.requires _).expects(*,*).returns(Set())
    (sourceRelation.partitions _).expects().returns(Seq(PartitionField("pcol", StringType)))

    (targetRelation.name _).expects().atLeastOnce().returns("tgt")
    (targetRelation.kind _).expects().atLeastOnce().returns("tgt_kind")
    (targetRelation.provides _).expects(*,*).returns(Set())
    (targetRelation.requires _).expects(*,*).returns(Set())

    (target.provides _).expects(*).returns(Set.empty)
    (target.requires _).expects(*).returns(Set.empty)
    (target.name _).expects().returns("tgt1")
    (target.kind _).expects().returns("tgt1_kind")

    val hgraph = Graph.ofGraph(graph)
    val srcRelNode = hgraph.nodes.find(_.name == "src").get
    val tgtRelNode = hgraph.nodes.find(_.name == "tgt").get
    val m1Node = hgraph.nodes.find(_.name == "m1").get
    val m2Node = hgraph.nodes.find(_.name == "m2").get
    val tgtNode = hgraph.nodes.find(_.name == "tgt1").get

    srcRelNode.name should be("src")
    srcRelNode.category should be(Category.RELATION)
    srcRelNode.kind should be("src_kind")
    srcRelNode.incoming should be(Seq.empty)
    srcRelNode.outgoing.head.input should be(srcRelNode)
    srcRelNode.outgoing.head.output should be(m2Node)

    m2Node.name should be("m2")
    m2Node.category should be(Category.MAPPING)
    m2Node.kind should be("m2_kind")
    m2Node.incoming.head.input should be(srcRelNode)
    m2Node.incoming.head.output should be(m2Node)
    m2Node.outgoing.head.input should be(m2Node)
    m2Node.outgoing.head.output should be(m1Node)

    m1Node.name should be("m1")
    m1Node.category should be(Category.MAPPING)
    m1Node.kind should be("m1_kind")
    m1Node.incoming.head.input should be(m2Node)
    m1Node.incoming.head.output should be(m1Node)
    m1Node.outgoing.head.input should be(m1Node)
    m1Node.outgoing.head.output should be(tgtNode)

    tgtNode.name should be("tgt1")
    tgtNode.category should be(Category.TARGET)
    tgtNode.kind should be("tgt1_kind")
    tgtNode.incoming.head.input should be(m1Node)
    tgtNode.incoming.head.output should be(tgtNode)
    tgtNode.outgoing.head.input should be(tgtNode)
    tgtNode.outgoing.head.output should be(tgtRelNode)

    tgtRelNode.name should be("tgt")
    tgtRelNode.category should be(Category.RELATION)
    tgtRelNode.kind should be("tgt_kind")
    tgtRelNode.outgoing should be(Seq.empty)
    tgtRelNode.incoming.head.input should be(tgtNode)
    tgtRelNode.incoming.head.output should be(tgtRelNode)

    session.shutdown()
  }
}
