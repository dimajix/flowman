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

import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue


sealed abstract class Edge {
    def input : Node
    def output : Node
    def action : String
}

case class ReadRelation(override val input:RelationRef, override val output:Node, partitions:Map[String,FieldValue] = Map()) extends Edge {
    override def action: String = s"READ from ${input.label} partitions=(${partitions.map(kv => kv._1 + "=" + kv._2).mkString(",")})"
    def resources : Set[ResourceIdentifier] = input.relation.resources(partitions)
}

case class InputMapping(override val input:MappingRef,override val output:Node,pin:String="main") extends Edge {
    override def action: String = s"INPUT from ${input.label} output '$pin'"
}

case class WriteRelation(override val input:Node, override val output:RelationRef, partition:Map[String,SingleValue] = Map()) extends Edge {
    override def action: String = s"WRITE from ${input.label} partition=(${partition.map(kv => kv._1 + "=" + kv._2).mkString(",")})"
    def resources : Set[ResourceIdentifier] = output.relation.resources(partition)
}
