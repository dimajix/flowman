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

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.Target


class NodeTest extends AnyFlatSpec with Matchers with MockFactory {
    "A Node" should "generate a nice tree" in {
        val srcRelation = mock[Relation]
        (srcRelation.kind _).expects().atLeastOnce().returns("files")
        (srcRelation.name _).expects().atLeastOnce().returns("source")
        val readMapping = mock[Mapping]
        (readMapping.kind _).expects().atLeastOnce().returns("read")
        (readMapping.name _).expects().atLeastOnce().returns("read_source")
        val mapping1 = mock[Mapping]
        (mapping1.kind _).expects().atLeastOnce().returns("select")
        (mapping1.name _).expects().atLeastOnce().returns("extract_data")
        val mapping2 = mock[Mapping]
        (mapping2.kind _).expects().atLeastOnce().returns("filter")
        (mapping2.name _).expects().atLeastOnce().returns("filter_data")
        val mapping3 = mock[Mapping]
        (mapping3.kind _).expects().atLeastOnce().returns("extend")
        (mapping3.name _).expects().atLeastOnce().returns("extend_data")
        val unionMapping = mock[Mapping]
        (unionMapping.kind _).expects().atLeastOnce().returns("union")
        (unionMapping.name _).expects().atLeastOnce().returns("union_all")
        val target = mock[Target]
        (target.kind _).expects().atLeastOnce().returns("relation")
        (target.name _).expects().atLeastOnce().returns("write_facts")
        val tgtRelation = mock[Relation]
        (tgtRelation.kind _).expects().atLeastOnce().returns("hive")
        (tgtRelation.name _).expects().atLeastOnce().returns("facts")

        val srcRelationNode = new RelationRef(1, srcRelation)
        val readMappingNode = new MappingRef(2, readMapping, Seq(new MappingOutput(3, "main")))
        val mapping1Node = new MappingRef(4, mapping1, Seq(new MappingOutput(5, "main")))
        val mapping2Node = new MappingRef(6, mapping2, Seq(new MappingOutput(7, "main")))
        val mapping3Node = new MappingRef(8, mapping3, Seq(new MappingOutput(9, "main")))
        val unionMappingNode = new MappingRef(10, unionMapping, Seq(new MappingOutput(11, "main")))
        val targetNode = new TargetRef(12, target, Phase.BUILD)
        val tgtRelationNode = new RelationRef(13, tgtRelation)

        tgtRelationNode.inEdges.append(WriteRelation(targetNode, tgtRelationNode))
        targetNode.inEdges.append(InputMapping(unionMappingNode.outputs.head, targetNode))
        unionMappingNode.inEdges.append(InputMapping(mapping1Node.outputs.head, unionMappingNode))
        unionMappingNode.inEdges.append(InputMapping(mapping2Node.outputs.head, unionMappingNode))
        unionMappingNode.inEdges.append(InputMapping(mapping3Node.outputs.head, unionMappingNode))
        mapping1Node.inEdges.append(InputMapping(readMappingNode.outputs.head, mapping1Node))
        mapping2Node.inEdges.append(InputMapping(readMappingNode.outputs.head, mapping2Node))
        mapping3Node.inEdges.append(InputMapping(readMappingNode.outputs.head, mapping3Node))
        readMappingNode.inEdges.append(ReadRelation(srcRelationNode, readMappingNode))

        println(tgtRelationNode.upstreamDependencyTree)
    }
}
