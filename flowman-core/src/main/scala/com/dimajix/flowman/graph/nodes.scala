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

import scala.collection.mutable

import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target


sealed abstract class Node {
    private[graph] val inEdges = mutable.Buffer[Edge]()
    private[graph] val outEdges = mutable.Buffer[Edge]()
    private[graph] val _parent : Option[Node] = None
    private[graph] val _children = mutable.Seq[Node]()

    def category : String
    def kind : String
    def name : String

    def incoming : Seq[Edge] = inEdges
    def outgoing : Seq[Edge] = outEdges
    def parent : Option[Node] = _parent
    def children : Seq[Node] = _children

    def upstreamDependencyTree : String = {
        def indentSubtree(lines:Iterator[String], margin:Boolean) : Iterator[String] = {
            if (lines.nonEmpty) {
                val prefix = if (margin) "  |  " else "     "
                val firstLine = "  +- " + lines.next()
                Iterator(firstLine) ++ lines.map(prefix + _)
            }
            else {
                Iterator()
            }
        }
        val trees = incoming.map { child =>
            child.action + " " + child.input.upstreamDependencyTree
        }
        val headChildren = trees.dropRight(1)
        val lastChild = trees.takeRight(1)

        val headTree = headChildren.flatMap(l => indentSubtree(l.linesIterator, true))
        val tailTree = lastChild.flatMap(l => indentSubtree(l.linesIterator, false))
        val root = s"$category[$kind]: $name"
        (Seq(root) ++ headTree ++ tailTree).mkString("\n")
    }
}

case class MappingRef(mapping:Mapping) extends Node {
    override def category: String = "mapping"
    override def kind: String = mapping.kind
    override def name: String = mapping.name
}
case class TargetRef(target:Target) extends Node {
    override def category: String = "target"
    override def kind: String = target.kind
    override def name: String = target.name
}
case class RelationRef(relation:Relation) extends Node {
    override def category: String = "relation"
    override def kind: String = relation.kind
    override def name: String = relation.name

    def resources : Set[ResourceIdentifier] = relation.resources(Map()) ++ relation.provides
}
case class MappingColumn(mapping: Mapping, output:String, column:String) extends Node {
    override def category: String = "mapping_column"
    override def kind: String = "mapping_column"
    override def name: String = mapping.name + "." + output + "." + column
}
case class RelationColumn(relation: Relation, column:String) extends Node {
    override def category: String = "relation_column"
    override def kind: String = "relation_column"
    override def name: String = relation.name + "." + column
}
