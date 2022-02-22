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

package com.dimajix.flowman.documentation

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Execution
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


class MappingCollectorTest extends AnyFlatSpec with Matchers with MockFactory {
    "MappingCollector.collect" should "work" in {
        val mapping1 = mock[Mapping]
        val mappingTemplate1 = mock[Prototype[Mapping]]
        val mapping2 = mock[Mapping]
        val mappingTemplate2 = mock[Prototype[Mapping]]
        val sourceRelation = mock[Relation]
        val sourceRelationTemplate = mock[Prototype[Relation]]

        val project = Project(
            name = "project",
            mappings = Map(
                "m1" -> mappingTemplate1,
                "m2" -> mappingTemplate2
            ),
            relations = Map(
                "src" -> sourceRelationTemplate
            )
        )
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)
        val execution = session.execution

        (mappingTemplate1.instantiate _).expects(context).returns(mapping1)
        (mapping1.context _).expects().returns(context)
        (mapping1.outputs _).expects().returns(Set("main"))
        (mapping1.link _).expects(*).onCall((l:Linker) => Some(1).foreach(_ => l.input(MappingIdentifier("m2"), "main")))

        (mappingTemplate2.instantiate _).expects(context).returns(mapping2)
        (mapping2.context _).expects().returns(context)
        (mapping2.outputs _).expects().returns(Set("main"))
        (mapping2.link _).expects(*).onCall((l:Linker) => Some(1).foreach(_ => l.read(RelationIdentifier("src"), Map("pcol"-> SingleValue("part1")))))

        (sourceRelationTemplate.instantiate _).expects(context).returns(sourceRelation)
        (sourceRelation.context _).expects().returns(context)
        (sourceRelation.link _).expects(*).returns(Unit)

        val graph = Graph.ofProject(session, project, Phase.BUILD)

        (mapping1.identifier _).expects().atLeastOnce().returns(MappingIdentifier("project/m1"))
        (mapping1.inputs _).expects().returns(Set(MappingOutputIdentifier("project/m2")))
        (mapping1.describe: (Execution,Map[MappingOutputIdentifier,StructType]) => Map[String,StructType] ).expects(*,*).returns(Map("main" -> StructType(Seq())))
        (mapping1.documentation _).expects().returns(None)
        (mapping1.context _).expects().returns(context)
        (mapping2.identifier _).expects().atLeastOnce().returns(MappingIdentifier("project/m2"))
        (mapping2.inputs _).expects().returns(Set())
        (mapping2.describe: (Execution,Map[MappingOutputIdentifier,StructType]) => Map[String,StructType] ).expects(*,*).returns(Map("main" -> StructType(Seq())))
        (mapping2.documentation _).expects().returns(None)

        val collector = new MappingCollector()
        val projectDoc = collector.collect(execution, graph, ProjectDoc(project.name))

        val mapping1Doc = projectDoc.mappings.find(_.identifier == RelationIdentifier("project/m1"))
        val mapping2Doc = projectDoc.mappings.find(_.identifier == RelationIdentifier("project/m2"))

        mapping1Doc should be (Some(MappingDoc(
            parent = Some(ProjectReference("project")),
            identifier = MappingIdentifier("project/m1"),
            inputs = Seq(MappingOutputReference(Some(MappingReference(Some(ProjectReference("project")), "m2")), "main")),
            outputs = Seq(
                MappingOutputDoc(
                    parent = Some(MappingReference(Some(ProjectReference("project")), "m1")),
                    identifier = MappingOutputIdentifier("project/m1:main"),
                    schema = Some(SchemaDoc(
                        parent = Some(MappingOutputReference(Some(MappingReference(Some(ProjectReference("project")), "m1")), "main"))
                    ))
                ))
        )))
        mapping2Doc should be (Some(MappingDoc(
            parent = Some(ProjectReference("project")),
            identifier = MappingIdentifier("project/m2"),
            inputs = Seq(),
            outputs = Seq(
                MappingOutputDoc(
                    parent = Some(MappingReference(Some(ProjectReference("project")), "m2")),
                    identifier = MappingOutputIdentifier("project/m2:main"),
                    schema = Some(SchemaDoc(
                        parent = Some(MappingOutputReference(Some(MappingReference(Some(ProjectReference("project")), "m2")), "main"))
                    ))
                ))
        )))
    }
}
