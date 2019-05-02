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

import com.google.common.base.CaseFormat
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.transforms.schema.ArrayNode
import com.dimajix.flowman.transforms.schema.ColumnTree
import com.dimajix.flowman.transforms.schema.LeafNode
import com.dimajix.flowman.transforms.schema.MapNode
import com.dimajix.flowman.transforms.schema.Node
import com.dimajix.flowman.transforms.schema.NodeOps
import com.dimajix.flowman.transforms.schema.SchemaTree
import com.dimajix.flowman.transforms.schema.StructNode
import com.dimajix.flowman.types.StructType


case class CaseFormatter(inputFormat:CaseFormat, outputFormat:CaseFormat) {
    import com.dimajix.flowman.transforms.schema.ColumnTree.implicits._
    import com.dimajix.flowman.transforms.schema.SchemaTree.implicits._

    def transform[T](root:Node[T])(implicit ops:NodeOps[T]) : Node[T] = {
        def processStruct(node:StructNode[T]) : StructNode[T] = {
            val children = node.children
            val newChildren = children.map(processNode)
            val newName = inputFormat.to(outputFormat, node.name)

            node.withName(newName).withChildren(newChildren)
        }
        def processMap(node:MapNode[T]) : MapNode[T] = {
            val newKey = processNode(node.mapKey)
            val newValue = processNode(node.mapValue)
            val newName = inputFormat.to(outputFormat, node.name)

            node.withName(newName).withKeyValue(newKey, newValue)
        }
        def processArray(node:ArrayNode[T]) : ArrayNode[T] = {
            val newElement = processNode(node.elements)
            val newName = inputFormat.to(outputFormat, node.name)

            node.withName(newName).withElements(newElement)
        }
        def processLeaf(node:LeafNode[T]) : LeafNode[T] = {
            val newName = inputFormat.to(outputFormat, node.name)
            node.withName(newName)
        }
        def processNode(node:Node[T]) : Node[T] = {
            node match {
                case st:StructNode[T] => processStruct(st)
                case at:ArrayNode[T] => processArray(at)
                case mt:MapNode[T] => processMap(mt)
                case leaf:LeafNode[T] => processLeaf(leaf)
            }
        }

        processNode(root)
    }

    /**
      * Transforms the field names of a dataframe to the desired case format
      * @param df
      * @return
      */
    def transform(df:DataFrame) : DataFrame = {
        val tree = ColumnTree.ofSchema(df.schema)
        val newTree = transform(tree)
        val columns = newTree.children.map(_.mkValue())
        df.select(columns:_*)
    }

    /**
      * Transforms the field names of a schema to the desired case format
      * @param schema
      * @return
      */
    def transform(schema:StructType) : StructType = {
        val tree = SchemaTree.ofSchema(schema)
        val newTree = transform(tree)
        val columns = newTree.children.map(_.mkValue())
        StructType(columns)
    }
}
