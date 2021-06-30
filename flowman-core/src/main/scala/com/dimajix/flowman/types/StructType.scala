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

package com.dimajix.flowman.types

import org.codehaus.jackson.annotate.JsonProperty


object StructType {
    def of(schema:org.apache.spark.sql.types.StructType) : StructType = {
        StructType(Field.of(schema.fields))
    }
}


case class StructType(@JsonProperty(value = "fields") fields:Seq[Field]) extends ContainerType {
    def this() = { this(Seq()) }

    /**
     * The Spark type to use
     */
    override def sparkType : org.apache.spark.sql.types.StructType = {
        org.apache.spark.sql.types.StructType(fields.map(_.sparkField))
    }

    /**
     * The type to use in catalogs like Hive etc. In contrast to [[sparkType]], this method will keep VarChartype
     * and similar types, which are not used in Spark itself
     */
    override def catalogType : org.apache.spark.sql.types.StructType = {
        org.apache.spark.sql.types.StructType(fields.map(_.catalogField))
    }

    /**
     * Short Type Name as used in SQL and in YAML specification files
     * @return
     */
    override def sqlType : String = {
        "struct<" + fields.map(f => f.name + ":" + f.sqlType).mkString(",") + ">"
    }
    override def parse(value:String, granularity:Option[String]=None) : Any = ???
    override def interpolate(value: FieldValue, granularity:Option[String]=None) : Iterable[Any] = ???

    /**
      * Provides a human readable string representation of the schema
      */
    def printTree() : Unit = {
        println(treeString)
    }

    /**
      * Provides a human readable string representation of the schema
      */
    def treeString : String = {
        val builder = new StringBuilder
        builder.append("root\n")
        val prefix = " |"
        fields.foreach(field => buildTreeString(field, prefix, builder))

        builder.toString()
    }

    private def buildTreeString(field:Field, prefix:String, builder:StringBuilder) : Unit = {
        builder.append(s"$prefix-- ${field.name}: ${field.typeName} (nullable = ${field.nullable})\n")
        buildTreeString(field.ftype, s"$prefix    |", builder)
    }

    private def buildTreeString(ftype:FieldType, prefix:String, builder:StringBuilder) : Unit = {
        ftype match {
            case struct:StructType =>
                struct.fields.foreach(field => buildTreeString(field, prefix, builder))
            case map:MapType =>
                builder.append(s"$prefix-- key: ${map.keyType.typeName}\n")
                builder.append(s"$prefix-- value: ${map.valueType.typeName} " +
                    s"(containsNull = ${map.containsNull})\n")
                buildTreeString(map.keyType, s"$prefix    |", builder)
                buildTreeString(map.valueType, s"$prefix    |", builder)
            case array:ArrayType =>
                builder.append(
                    s"$prefix-- element: ${array.elementType.typeName} (containsNull = ${array.containsNull})\n")
                buildTreeString(array.elementType, s"$prefix    |", builder)
            case _ =>
        }
    }
}
