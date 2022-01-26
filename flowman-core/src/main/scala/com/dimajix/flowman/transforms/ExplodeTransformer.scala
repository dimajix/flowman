/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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
import com.dimajix.flowman.transforms.schema.Node
import com.dimajix.flowman.transforms.schema.NodeOps
import com.dimajix.flowman.transforms.schema.Path
import com.dimajix.flowman.transforms.schema.StructNode
import com.dimajix.flowman.transforms.schema.TreeTransformer


final case class ExplodeTransformer(
    array:Path,
    keep:Seq[Path],
    drop:Seq[Path],
    rename:Map[String,Path]
) extends TreeTransformer {
    private def explode[T](root:Node[T])(implicit ops:NodeOps[T]) : Seq[Node[T]] = {
        val pruned =
            if (keep.nonEmpty) {
                root.keep(keep)
                    .drop(drop :+ array)
                    .children
            }
            else {
                root.drop(drop :+ array)
                    .children
            }

        val renamed = rename.flatMap(kv => root.find(kv._2).map(_.withName(kv._1)))

        val node = root
            .find(array)
            .getOrElse(throw new IllegalArgumentException(s"Array '$array' not found in explode"))
        val elements = node match {
            case an:ArrayNode[T] => an.elements
            case n:Node[T] => n
        }
        val exploded = Seq(elements.withName(node.name).withValue(ops.explode(node.name, node.mkValue())))

        pruned ++ renamed ++ exploded
    }

    override def transform[T](root:Node[T])(implicit ops:NodeOps[T]) : Node[T] = {
        val newChildren = explode(root)
        root.asInstanceOf[StructNode[T]].withChildren(newChildren)
    }
}
