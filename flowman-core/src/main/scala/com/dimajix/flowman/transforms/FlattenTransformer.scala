/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.transforms

import com.dimajix.flowman.transforms.schema.ArrayNode
import com.dimajix.flowman.transforms.schema.LeafNode
import com.dimajix.flowman.transforms.schema.MapNode
import com.dimajix.flowman.transforms.schema.Node
import com.dimajix.flowman.transforms.schema.NodeOps
import com.dimajix.flowman.transforms.schema.StructNode
import com.dimajix.flowman.transforms.schema.TreeTransformer


case class FlattenTransformer(format:CaseFormat) extends TreeTransformer {
    private def rename(prefix:String, name:String) : String = {
        format.concat(prefix, name)
    }

    private def flatten[T](node:Node[T], prefix:String) : Seq[Node[T]] = {
        node match {
            case leaf:LeafNode[T] => Seq(leaf.withName(rename(prefix, leaf.name)))
            case struct:StructNode[T] => struct.children.flatMap(flatten(_, rename(prefix, struct.name)))
            case array:ArrayNode[T] => Seq(array.withName(rename(prefix , array.name)))
            case map:MapNode[T] => Seq(map.withName(rename(prefix, map.name)))
        }
    }

    override def transform[T](root:Node[T])(implicit ops:NodeOps[T]) : Node[T] = {
        val newChildren = root.children.flatMap(flatten(_, ""))
        root.asInstanceOf[StructNode[T]].withChildren(newChildren)
    }
}
