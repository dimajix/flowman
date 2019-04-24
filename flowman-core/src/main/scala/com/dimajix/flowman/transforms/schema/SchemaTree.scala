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


class SchemaNodeOps extends NodeOps[Field] {
    override def empty : Field = Field("", NullType)

    override def metadata(value:Field, meta:Map[String,String]) : Field = {
        val description = if (meta.contains("comment")) meta("comment") else value.description
        val default = if (meta.contains("default")) meta("default") else value.default
        val format = if (meta.contains("format")) meta("format") else value.format
        val size = if (meta.contains("size")) meta("size").toInt else value.size
        Field(
            value.name,
            value.ftype,
            value.nullable,
            description,
            default,
            if (size > 0) Some(size) else None,
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
                if (value.size > 0) Some(value.size) else None,
                value.format
            )
        }
    }

    override def struct(name:String, children:Seq[Field], nullable:Boolean) : Field = {
        Field(name, StructType(children), nullable)
    }

    override def struct_pruned(name:String, children:Seq[Field], nullable:Boolean) : Field = {
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


object SchemaTree {
    object implicits {
        implicit val schemaNodeOps = new SchemaNodeOps
    }

    def ofSchema(schema:StructType) : Node[Field] = {
        def processField(field: Field) : Node[Field] = {
            val node = field.ftype match {
                case st:StructType => processStruct(field.name, st)
                case at:ArrayType => processArray(field.name, at)
                case _:FieldType => processLeaf(field)
            }

            val metadata = mutable.Map[String,String]()
            if (field.description != null)
                metadata.update("comment", field.description)
            if (field.default != null)
                metadata.update("default", field.default)
            if (field.format != null)
                metadata.update("format", field.format)
            if (field.size > 0)
                metadata.update("size", field.size.toString)

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
