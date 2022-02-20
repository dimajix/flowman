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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.IdentifierRelationReference
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ValueRelationReference
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue


final case class Linker private[graph](builder:GraphBuilder, context:Context, node:Node) {
    def input(mapping: Mapping, output:String) : Linker = {
        val in = builder.refMapping(mapping)
        val out = in.outputs.find(_.output == output)
            .getOrElse(throw new IllegalArgumentException(s"Mapping '${mapping.identifier}' doesn't provide output '$output'"))
        val edge = InputMapping(out, node)
        link(edge)
    }
    def input(mapping: MappingIdentifier, output:String) : Linker = {
        val instance = context.getMapping(mapping)
        input(instance, output)
    }

    def read(relation: Reference[Relation], partitions:Map[String,FieldValue]) : Linker = {
        relation match {
            case ref:ValueRelationReference => read(ref.value, partitions)
            case ref:IdentifierRelationReference => read(ref.identifier, partitions)
        }
    }
    def read(relation: Relation, partitions:Map[String,FieldValue]) : Linker = {
        val in = builder.refRelation(relation)
        val edge = ReadRelation(in, node, partitions)
        link(edge)
    }
    def read(relation: RelationIdentifier, partitions:Map[String,FieldValue]) : Linker = {
        val instance = context.getRelation(relation)
        read(instance, partitions)
    }

    def write(relation: Reference[Relation], partitions:Map[String,SingleValue]) : Linker = {
        relation match {
            case ref:ValueRelationReference => write(ref.value, partitions)
            case ref:IdentifierRelationReference => write(ref.identifier, partitions)
        }
    }
    def write(relation: Relation, partition:Map[String,SingleValue]) : Linker = {
        val out = builder.refRelation(relation)
        val edge = WriteRelation(node, out, partition)
        link(edge)
    }
    def write(relation: RelationIdentifier, partition:Map[String,SingleValue]) : Linker = {
        val instance = context.getRelation(relation)
        write(instance, partition)
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
