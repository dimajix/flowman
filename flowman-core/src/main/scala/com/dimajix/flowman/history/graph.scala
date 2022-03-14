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

package com.dimajix.flowman.history

import scala.collection.mutable

import com.dimajix.common.MapIgnoreCase
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.graph.Action
import com.dimajix.flowman.graph.Category
import com.dimajix.flowman.{graph => g}
import com.dimajix.flowman.model.Target


object Graph {
    class Builder {
        private val nodes = mutable.Map[Int,Node]()
        private val edges = mutable.ArrayBuffer[Edge]()
        private var currentNodeId:Int = 1

        /**
         * Adds a new [[MappingNode]] to the graph
         * @param name
         * @param kind
         * @return
         */
        def newMappingNode(name:String, kind:String, requires:Seq[Resource]) : MappingNode = {
            val node = MappingNode(nextNodeId(), name, kind, Seq(), requires)
            addNode(node)
            node
        }

        /**
         * Adds a new [[TargetNode]] to the graph
         * @param name
         * @param kind
         * @return
         */
        def newTargetNode(name:String, kind:String, provides:Seq[Resource], requires:Seq[Resource]) : TargetNode = {
            val node = TargetNode(nextNodeId(), name, kind, provides, requires)
            addNode(node)
            node
        }

        /**
         * Adds a new [[RelationNode]] to the graph
         * @param name
         * @param kind
         * @return
         */
        def newRelationNode(name:String, kind:String, provides:Seq[Resource], requires:Seq[Resource]) : RelationNode = {
            val node = RelationNode(nextNodeId(), name, kind, provides, requires)
            addNode(node)
            node
        }

        def addNode(node:Node) : Unit = {
            nodes.put(node.id, node)
        }

        /**
         * Adds an [[Edge]] to the graph. The end points of the edge should be previsouly created with one of
         * the methods [[newMappingNode]], [[newTargetNode]] or [[newRelationNode]]
         * @param edge
         */
        def addEdge(edge:Edge) : Unit = {
            val in = edge.input
            val out = edge.output
            in._outgoing.append(edge)
            out._incoming.append(edge)
            edges.append(edge)
        }

        /**
         * Builds and returns the graph
         * @return
         */
        def build() : Graph = {
            Graph(
                nodes.values.toList,
                edges
            )
        }

        private def nextNodeId() : Int = {
            val result = currentNodeId
            currentNodeId += 1
            result
        }
    }

    /**
     * Creates a new Builder
     * @return
     */
    def builder() : Builder = new Builder()

    /**
     * Creates a history graph from a given [[Target]]. Internally this uses the [[g.GraphBuilder]] for creating
     * the graph and then converts the result to the history [[Graph]]
     * @param target
     * @return
     */
    def ofTarget(target:Target, phase:Phase) : Graph = {
        val context = target.context
        val graph = new g.GraphBuilder(context, phase).addTarget(target).build()
        ofGraph(graph)
    }

    /**
     * Creates a history graph from a given [[g.Graph]].
     * @param graph
     * @return
     */
    def ofGraph(graph:g.Graph) : Graph = {
        val builder = Graph.builder()
        val nodesById = graph.nodes.flatMap {
            case target:g.TargetRef =>
                val provides = target.provides.map(r => Resource(r.category, r.name, r.partition)).toSeq
                val requires = target.requires.map(r => Resource(r.category, r.name, r.partition)).toSeq
                Some(target.id -> builder.newTargetNode(target.name, target.kind, provides, requires))
            case mapping:g.MappingRef =>
                val requires = mapping.requires.map(r => Resource(r.category, r.name, r.partition)).toSeq
                Some(mapping.id -> builder.newMappingNode(mapping.name, mapping.kind, requires))
            case relation:g.RelationRef =>
                val provides = relation.provides.map(r => Resource(r.category, r.name, r.partition)).toSeq
                val requires = relation.requires.map(r => Resource(r.category, r.name, r.partition)).toSeq
                Some(relation.id -> builder.newRelationNode(relation.name, relation.kind, provides, requires))
            case _ => None
        }.toMap

        val relationsById = graph.nodes.collect {
            case relation:g.RelationRef =>
                relation.id -> relation.relation
        }.toMap

        graph.edges.foreach {
            case read:g.ReadRelation =>
                val in = nodesById(read.input.id).asInstanceOf[RelationNode]
                val out = nodesById(read.output.id)
                val relation = relationsById(read.input.id)
                val partitionFields = MapIgnoreCase(relation.partitions.map(p => p.name -> p))
                val p = read.partitions.map { case(k,v) => (k -> partitionFields(k).interpolate(v).map(_.toString).toSeq) }
                builder.addEdge(ReadRelation(in, out, p))
            case map:g.InputMapping =>
                val in = nodesById(map.mapping.id).asInstanceOf[MappingNode]
                val out = nodesById(map.output.id)
                builder.addEdge(InputMapping(in, out, map.pin))
            case write:g.WriteRelation =>
                val in = nodesById(write.input.id)
                val out = nodesById(write.output.id).asInstanceOf[RelationNode]
                val p = write.partition.map { case(k,v) => (k -> v.value) }
                builder.addEdge(WriteRelation(in, out, p))
            case _ =>
        }

        builder.build()
    }
}

final case class Graph(nodes:Seq[Node], edges:Seq[Edge]) {
    def subgraph(node:Node): Graph = {
        val builder = Graph.builder()
        val nodesById = mutable.Map[Int,Node]()
        // IDs of processed nodes
        val incomingIds = mutable.Set[Int]()
        val outgoingIds = mutable.Set[Int]()

        def replaceIncoming(node:Node) : Node = {
            if (incomingIds.contains(node.id)) {
                nodesById(node.id)
            }
            else {
                val newNode = nodesById.getOrElseUpdate(node.id, node.withoutEdges)
                builder.addNode(newNode)
                incomingIds.add(newNode.id)
                node.incoming.foreach { e =>
                    val input = replaceIncoming(e.input)
                    val edge = e.withNodes(input, newNode)
                    builder.addEdge(edge)
                }
                newNode
            }
        }

        def replaceOutgoing(node:Node) : Node = {
            if (outgoingIds.contains(node.id)) {
                nodesById(node.id)
            }
            else {
                val newNode = nodesById.getOrElseUpdate(node.id, node.withoutEdges)
                builder.addNode(newNode)
                outgoingIds.add(newNode.id)
                node.outgoing.foreach { e =>
                    val output = replaceOutgoing(e.output)
                    val edge = e.withNodes(newNode, output)
                    builder.addEdge(edge)
                }
                newNode
            }
        }

        replaceIncoming(node)
        replaceOutgoing(node)

        builder.build()
    }
}


final case class Resource(
    category: String,
    name: String,
    partition: Map[String,String]
)


sealed abstract class Node {
    /**
     * Node id. This is unique within a single graph
     * @return
     */
    def id : Int

    /**
     * Textual label for displaying
     * @return
     */
    def description : String = s"($id) ${category.lower}/$kind: '$name'"

    def category : Category
    def kind : String
    def name : String
    def incoming: Seq[Edge] = _incoming
    def outgoing: Seq[Edge] = _outgoing

    def provides: Seq[Resource]
    def requires: Seq[Resource]

    def withoutEdges : Node

    /**
     * Create a nice string representation of the upstream dependency tree
     * @return
     */
    def upstreamDependencyTree : String = {
        description + "\n" + upstreamTreeRec
    }

    private def upstreamTreeRec : String = {
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
            child.description + "\n" + child.input.upstreamTreeRec
        }
        val headChildren = trees.dropRight(1)
        val lastChild = trees.takeRight(1)

        val headTree = headChildren.flatMap(l => indentSubtree(l.linesIterator, true))
        val tailTree = lastChild.flatMap(l => indentSubtree(l.linesIterator, false))
        (headTree ++ tailTree).mkString("\n")
    }

    private[history] val _incoming = mutable.ArrayBuffer[Edge]()
    private[history] val _outgoing = mutable.ArrayBuffer[Edge]()
}


final case class TargetNode(
    override val id:Int,
    override val name:String,
    override val kind:String,
    override val provides:Seq[Resource] = Seq(),
    override val requires:Seq[Resource] = Seq()
) extends Node {
    override def category: Category = Category.TARGET
    override def withoutEdges : Node = TargetNode(id, name, kind, provides, requires)
}

final case class MappingNode(
    override val id:Int,
    override val name:String,
    override val kind:String,
    override val provides:Seq[Resource] = Seq(),
    override val requires:Seq[Resource] = Seq()
) extends Node {
    override def category: Category = Category.MAPPING
    override def withoutEdges : Node = MappingNode(id, name, kind, provides, requires)
}

final case class RelationNode(
    override val id:Int,
    override val name:String,
    override val kind:String,
    override val provides:Seq[Resource] = Seq(),
    override val requires:Seq[Resource] = Seq()
) extends Node {
    override def category: Category = Category.RELATION
    override def withoutEdges : Node = RelationNode(id, name, kind, provides, requires)
}




sealed abstract class Edge {
    def input : Node
    def output : Node
    def action : Action
    def labels : Map[String,Seq[String]]
    def description : String

    def withNodes(input:Node, output:Node) : Edge
}

final case class ReadRelation(
    override val input:RelationNode,
    override val output:Node,
    partitions:Map[String,Seq[String]] = Map()
) extends Edge {
    override def action: Action = Action.READ
    override def labels: Map[String,Seq[String]] = partitions
    override def description: String = s"$action from ${input.description} partitions=(${partitions.map(kv => kv._1 + "=" + kv._2).mkString(",")})"

    override def withNodes(input: Node, output: Node): Edge = copy(input = input.asInstanceOf[RelationNode], output = output)
}

final case class InputMapping(
    override val input:MappingNode,
    override val output:Node,
    pin:String = "main"
) extends Edge {
    override def action: Action = Action.INPUT
    override def labels: Map[String,Seq[String]] = Map("pin" -> Seq(pin))
    override def description: String = s"$action from ${input.description} output '$pin'"

    override def withNodes(input: Node, output: Node): Edge = copy(input = input.asInstanceOf[MappingNode], output = output)
}

final case class WriteRelation(
    override val input:Node,
    override val output:RelationNode,
    partition:Map[String,String] = Map()
) extends Edge {
    override def action: Action = Action.WRITE
    override def labels: Map[String,Seq[String]] = partition.map { case(k,v) => k -> Seq(v) }
    override def description: String = s"$action from ${input.description} partition=(${partition.map(kv => kv._1 + "=" + kv._2).mkString(",")})"

    override def withNodes(input: Node, output: Node): Edge = copy(input = input, output = output.asInstanceOf[RelationNode])
}
