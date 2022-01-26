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

import com.dimajix.flowman.transforms.schema.Node
import com.dimajix.flowman.transforms.schema.NodeOps
import com.dimajix.flowman.transforms.schema.Path
import com.dimajix.flowman.transforms.schema.StructNode
import com.dimajix.flowman.transforms.schema.TreeTransformer


final case class LiftTransformer(
    path:Path,
    keep:Seq[Path],
    drop:Seq[Path],
    rename:Map[String,Path]
) extends TreeTransformer {
    private def lift[T](root:Node[T]) : Seq[Node[T]] = {
        val nested = root.find(path).get
        val outer = root.drop(path).children
        val pruned =
            if (keep.nonEmpty) {
                nested.keep(keep)
                    .drop(drop)
                    .children
            }
            else {
                nested.drop(drop)
                    .children
            }

        val renamed = rename.flatMap(kv => nested.find(kv._2).map(_.withName(kv._1)))

        outer ++ pruned ++ renamed
    }

    override def transform[T](root:Node[T])(implicit ops:NodeOps[T]) : Node[T] = {
        val newChildren = lift(root)
        root.asInstanceOf[StructNode[T]].withChildren(newChildren)
    }
}
