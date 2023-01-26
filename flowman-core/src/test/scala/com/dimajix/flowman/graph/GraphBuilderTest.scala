/*
 * Copyright 2021-2023 Kaya Kupferschmidt
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
import com.dimajix.flowman.types.StructType


class GraphBuilderTest extends AnyFlatSpec with Matchers with MockFactory {
    "The GraphBuilder" should "work" in {
        val mapping1 = mock[Mapping]
        val mappingTemplate1 = mock[Prototype[Mapping]]
        val mapping2 = mock[Mapping]
        val mappingTemplate2 = mock[Prototype[Mapping]]

        val project = Project(
            name = "project",
            mappings = Map(
                "m1" -> mappingTemplate1,
                "m2" -> mappingTemplate2
            )
        )
        val session = Session.builder()
            .withProject(project)
            .disableSpark()
            .build()
        val context = session.getContext(project)

        (mappingTemplate1.instantiate _).expects(context,None).returns(mapping1)
        (mapping1.identifier _).expects().returns(MappingIdentifier("project/m1"))
        (mapping1.context _).expects().atLeastOnce().returns(context)
        (mapping1.outputs _).expects().atLeastOnce().returns(Set("main"))
        (mapping1.inputs _).expects().returns(Set(MappingOutputIdentifier("m2")))
        (mapping1.kind _).expects().returns("m1_kind")
        (mapping1.name _).expects().atLeastOnce().returns("m1")
        (mapping1.describe _).expects(*,*).returns(Map("main" -> StructType(Seq.empty)))
        (mapping1.link _).expects(*).onCall((l:Linker) => Some(1).foreach(_ => l.input(MappingIdentifier("m2"), "main")))
        (mappingTemplate2.instantiate _).expects(context,None).returns(mapping2)
        (mapping2.identifier _).expects().atLeastOnce().returns(MappingIdentifier("project/m2"))
        (mapping2.context _).expects().atLeastOnce().returns(context)
        (mapping2.outputs _).expects().atLeastOnce().returns(Set("main"))
        (mapping2.inputs _).expects().atLeastOnce().returns(Set())
        (mapping2.kind _).expects().returns("m2_kind")
        (mapping2.name _).expects().atLeastOnce().returns("m2")
        (mapping2.describe _).expects(*,*).returns(Map("main" -> StructType(Seq.empty)))
        (mapping2.link _).expects(*).returns(Unit)

        val graph = new GraphBuilder(context, Phase.BUILD)
            .addMapping(MappingIdentifier("m1"))
            .build()

        val nodes = graph.nodes

        val ref1 = nodes.find(_.name == "m1").head.asInstanceOf[MappingRef]
        val ref2 = nodes.find(_.name == "m2").head.asInstanceOf[MappingRef]
        val out2main = ref2.outputs.head

        ref1.category should be (Category.MAPPING)
        ref1.kind should be ("m1_kind")
        ref1.name should be ("m1")
        ref1.mapping should be (mapping1)
        ref1.incoming should be (Seq(
            InputMapping(out2main, ref1)
        ))
        ref1.outgoing should be (Seq())

        ref2.category should be (Category.MAPPING)
        ref2.kind should be ("m2_kind")
        ref2.name should be ("m2")
        ref2.mapping should be (mapping2)
        ref2.incoming should be (Seq())
        ref2.outgoing should be (Seq())
        ref2.outputs.head.outgoing should be (Seq(
            InputMapping(out2main, ref1)
        ))

        session.shutdown()
    }
}
