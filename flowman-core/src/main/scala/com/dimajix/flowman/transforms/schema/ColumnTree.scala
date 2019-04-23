/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.transforms.schema

import scala.collection.mutable

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType


class ColumnNodeOps extends NodeOps[Column] {
    override def empty : Column = col("")

    override def metadata(value:Column, meta:Map[String,String]) : Column = value

    override def nullable(value:Column, n:Boolean) : Column = value

    override def leaf(name:String, value:Column) : Column = withName(name, value)

    override def struct(name:String, children:Seq[Column]) : Column = {
        withName(name, functions.struct(children: _*))
    }

    override def array(name:String, element:Column) : Column = ???

    override def map(name:String, keyType:Column, valueType:Column) : Column = ???

    private def withName(name:String, value:Column) : Column = {
        if (name.nonEmpty)
            value.as(name)
        else
            value
    }
}


object ColumnTree {
    object implicits {
        implicit val columnNodeOps = new ColumnNodeOps
    }

    def ofSchema(schema:StructType) : Node[Column] = {
        def fq(prefix:String, name:String) : String = {
            if (prefix.isEmpty)
                name
            else if (name.isEmpty)
                prefix
            else
                prefix + "." + name
        }
        def processField(prefix:String, field: StructField) : Node[Column] = {
            val node = field.dataType match {
                case st:StructType => processStruct(prefix, field.name, st)
                // Arrays are not supported as dedicated nodes
                //case at:ArrayType => processArray(prefix, field.name, at)
                case _:DataType => processLeaf(prefix, field.name)
            }

            val metadata = mutable.Map[String,String]()
            if (field.metadata.contains("comment"))
                metadata.update("comment", field.metadata.getString("comment"))
            if (field.metadata.contains("default"))
                metadata.update("default", field.metadata.getString("default"))
            if (field.metadata.contains("format"))
                metadata.update("format", field.metadata.getString("format"))
            if (field.metadata.contains("size"))
                metadata.update("size", field.metadata.getLong("size").toString)

            node.withNullable(field.nullable)
                .withMetadata(metadata.toMap)
        }
        def processStruct(prefix:String, name:String, st:StructType) : StructNode[Column] = {
            val fqName = fq(prefix, name)
            val children = st.fields.map(field => processField(fqName, field))
            StructNode(name, children)
        }
        def processArray(prefix:String, name:String, at:ArrayType) : ArrayNode[Column] = {
            val fqName = fq(prefix, name)
            val elem = at.elementType match {
                case st:StructType => processStruct(fqName, "element", st)
                case at:ArrayType => processArray(fqName, "element", at)
                case _:DataType => processLeaf(fqName, "element")
            }
            ArrayNode(name, elem)
        }
        def processLeaf(prefix:String, name:String) : LeafNode[Column] = {
            val fqName = fq(prefix, name)
            LeafNode(name, col(fqName))
        }

        processStruct("", "", schema)
    }
}
