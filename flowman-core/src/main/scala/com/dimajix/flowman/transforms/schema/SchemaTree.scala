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

import com.dimajix.flowman.types.ArrayType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.MapType
import com.dimajix.flowman.types.NullType
import com.dimajix.flowman.types.StructType


/**
  * Implementation of NodeOps required for manipulating a tree of [[com.dimajix.flowman.types.Field]] nodes
  * which represent a tree of a Flowman schema
  */
class SchemaNodeOps extends NodeOps[Field] {
    override def empty : Field = Field("", NullType)

    override def metadata(value:Field, meta:Map[String,String]) : Field = {
        val description = meta.get("comment").orElse(value.description)
        val default = meta.get("default").orElse(value.default)
        val format = meta.get("format").orElse(value.format)
        val size = meta.get("size").map(_.toInt).orElse(value.size)
        Field(
            value.name,
            value.ftype,
            value.nullable,
            description,
            default,
            size,
            format
        )
    }

    override def leaf(name:String, value:Field, nullable:Boolean) : Field = {
        if (name.isEmpty) {
            value
        }
        else {
            Field(
                name,
                value.ftype,
                nullable,
                value.description,
                value.default,
                value.size,
                value.format
            )
        }
    }

    override def struct(name:String, children:Seq[Field], nullable:Boolean) : Field = {
        require(children.nonEmpty)
        Field(name, StructType(children), nullable)
    }

    override def struct_pruned(name:String, children:Seq[Field], nullable:Boolean) : Field = {
        require(children.nonEmpty)
        Field(name, StructType(children), nullable)
    }

    override def array(name:String, element:Field, nullable:Boolean) : Field = {
        Field(name, ArrayType(element.ftype), nullable)
    }

    override def map(name:String, keyType:Field, valueType:Field, nullable:Boolean) : Field = {
        Field(name, MapType(keyType.ftype, valueType.ftype), nullable)
    }

    override def explode(name: String, array: Field): Field = {
        array.ftype match {
            case at:ArrayType => Field(name, at.elementType, at.containsNull || array.nullable, array.description)
            case _ => array.copy(name=name)
        }
    }
}


/**
  * Utility class for creating a tree with nodes of type [[com.dimajix.flowman.types.Field]] for manipulating a
  * Flowman schema
  */
object SchemaTree {
    object implicits {
        implicit val schemaNodeOps = new SchemaNodeOps
    }

    /**
      * Construct a tree with `Node[Field]` elements from a Flowman StructType.
      * @param schema
      * @return
      */
    def ofSchema(schema:StructType) : Node[Field] = {
        def processField(field: Field) : Node[Field] = {
            val node = field.ftype match {
                case st:StructType => processStruct(field.name, st)
                case at:ArrayType => processArray(field.name, at)
                case _:FieldType => processLeaf(field)
            }

            val metadata = mutable.Map[String,String]()
            field.description.foreach(metadata.update("comment", _))
            field.default.foreach(metadata.update("default", _))
            field.format.foreach(metadata.update("format", _))
            field.size.foreach(s => metadata.update("size", s.toString))

            node.withNullable(field.nullable)
                .withMetadata(metadata.toMap)
        }
        def processStruct(name:String, st:StructType) : StructNode[Field] = {
            val children = st.fields.map(field => processField(field))
            StructNode(name, None, children)
        }
        def processArray(name:String, at:ArrayType) : ArrayNode[Field] = {
            val elem = at.elementType match {
                case st:StructType => processStruct("element", st)
                case at:ArrayType => processArray("element", at)
                case f:FieldType => processLeaf(Field("element", f))
            }
            ArrayNode(name, None, elem)
        }
        def processLeaf(field:Field) : LeafNode[Field] = {
            LeafNode(field.name, field)
        }

        processStruct("", schema).withNullable(false)
    }
}
