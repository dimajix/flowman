/*
 * Copyright (C) 2022 The Flowman Authors
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

package com.dimajix.flowman.documentation

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.graph.Graph
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StructType


class RelationCollectorTest extends AnyFlatSpec with Matchers with MockFactory {
    "RelationCollector.collect" should "work" in {
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
        val execution = session.execution

        (mappingTemplate1.instantiate _).expects(context,None).returns(mapping1)
        (mapping1.identifier _).expects().atLeastOnce().returns(MappingIdentifier("project/m1"))
        (mapping1.context _).expects().atLeastOnce().returns(context)
        (mapping1.outputs _).expects().atLeastOnce().returns(Set("main"))
        (mapping1.inputs _).expects().returns(Set(MappingOutputIdentifier("m2")))
        (mapping1.describe _).expects(*,*).returns(Map("main" -> StructType(Seq.empty)))
        (mapping1.link _).expects(*).onCall((l:Linker) => Some(1).foreach(_ => l.input(MappingIdentifier("m2"), "main")))

        (mappingTemplate2.instantiate _).expects(context,None).returns(mapping2)
        (mapping2.identifier _).expects().atLeastOnce().returns(MappingIdentifier("project/m2"))
        (mapping2.context _).expects().atLeastOnce().returns(context)
        (mapping2.inputs _).expects().atLeastOnce().returns(Set())
        (mapping2.outputs _).expects().atLeastOnce().returns(Set("main"))
        (mapping2.describe _).expects(*,*).returns(Map("main" -> StructType(Seq.empty)))
        (mapping2.link _).expects(*).onCall((l:Linker) => Some(1).foreach(_ => l.read(RelationIdentifier("src"), Map("pcol"-> SingleValue("part1")))))

        (sourceRelationTemplate.instantiate _).expects(context,None).returns(sourceRelation)
        (sourceRelation.identifier _).expects().atLeastOnce().returns(RelationIdentifier("project/src"))
        (sourceRelation.context _).expects().returns(context)
        (sourceRelation.link _).expects(*).returns(Unit)
        (sourceRelation.describe _).expects(*,Map("pcol"-> SingleValue("part1"))).returns(StructType(Seq()))

        (targetRelationTemplate.instantiate _).expects(context,None).returns(targetRelation)
        (targetRelation.identifier _).expects().atLeastOnce().returns(RelationIdentifier("project/tgt"))
        (targetRelation.context _).expects().returns(context)
        (targetRelation.link _).expects(*).returns(Unit)
        (targetRelation.describe _).expects(*,Map("outcol"-> SingleValue("part1"))).returns(StructType(Seq()))

        (targetTemplate.instantiate _).expects(context,None).returns(target)
        (target.context _).expects().returns(context)
        (target.link _).expects(*,*).onCall((l:Linker, _:Phase) => Some(1).foreach { _ =>
            l.input(MappingIdentifier("m1"), "main")
            l.write(RelationIdentifier("tgt"), Map("outcol"-> SingleValue("part1")))
        })

        val graph = Graph.ofProject(session, project, Phase.BUILD)

        //(mapping1.identifier _).expects().atLeastOnce().returns(MappingIdentifier("project/m1"))
        //(mapping2.identifier _).expects().atLeastOnce().returns(MappingIdentifier("project/m2"))
        (mapping1.requires _).expects().returns(Set())
        (mapping2.requires _).expects().returns(Set())

        (sourceRelation.kind _).expects().atLeastOnce().returns("hive")
        (sourceRelation.description _).expects().atLeastOnce().returns(Some("source relation"))
        (sourceRelation.documentation _).expects().returns(None)
        (sourceRelation.provides _).expects(*,*).returns(Set())
        (sourceRelation.requires _).expects(*,*).returns(Set())
        (sourceRelation.schema _).expects().returns(None)

        (targetRelation.kind _).expects().atLeastOnce().returns("hive")
        (targetRelation.description _).expects().atLeastOnce().returns(Some("target relation"))
        (targetRelation.documentation _).expects().returns(None)
        (targetRelation.provides _).expects(*,*).returns(Set())
        (targetRelation.requires _).expects(*,*).returns(Set())
        (targetRelation.schema _).expects().returns(None)

        val collector = new RelationCollector()
        val projectDoc = collector.collect(execution, graph, ProjectDoc(project.name))

        val sourceRelationDoc = projectDoc.relations.find(_.identifier == RelationIdentifier("project/src"))
        val targetRelationDoc = projectDoc.relations.find(_.identifier == RelationIdentifier("project/tgt"))

        sourceRelationDoc should be (Some(RelationDoc(
            parent = Some(ProjectReference("project")),
            relation = Some(sourceRelation),
            description = Some("source relation"),
            schema = Some(SchemaDoc(
                parent = Some(RelationReference(Some(ProjectReference("project")), "src"))
            )),
            partitions = Map("pcol" -> SingleValue("part1"))
        )))

        targetRelationDoc should be (Some(RelationDoc(
            parent = Some(ProjectReference("project")),
            relation = Some(targetRelation),
            description = Some("target relation"),
            schema = Some(SchemaDoc(
                parent = Some(RelationReference(Some(ProjectReference("project")), "tgt"))
            )),
            inputs = Seq(MappingOutputReference(Some(MappingReference(Some(ProjectReference("project")), "m1")), "main")),
            partitions = Map("outcol" -> SingleValue("part1"))
        )))

        session.shutdown()
    }
}
