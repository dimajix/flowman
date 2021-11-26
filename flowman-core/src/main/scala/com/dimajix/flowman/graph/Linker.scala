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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue


final case class Linker private[graph](builder:GraphBuilder, context:Context, node:Node) {
    def input(mapping: MappingIdentifier, output:String) : Linker = {
        val instance = context.getMapping(mapping)
        val in = builder.refMapping(instance)
        val edge = InputMapping(in, node, output)
        link(edge)
    }
    def read(relation: RelationIdentifier, partitions:Map[String,FieldValue]) : Linker = {
        val instance = context.getRelation(relation)
        val in = builder.refRelation(instance)
        val edge = ReadRelation(in, node, partitions)
        link(edge)
    }
    def write(relation: RelationIdentifier, partition:Map[String,SingleValue]) : Linker = {
        val instance = context.getRelation(relation)
        val out = builder.refRelation(instance)
        val edge = WriteRelation(node, out, partition)
        link(edge)
    }

    /**
     * Performs a linking operation by adding an edge
     * @param edge
     */
    def link(edge:Edge) : Linker = {
        edge.input.outEdges.append(edge)
        edge.output.inEdges.append(edge)
        this
    }
}
