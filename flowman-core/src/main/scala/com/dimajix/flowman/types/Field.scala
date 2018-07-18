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

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.StructField


object Field {
    def apply(name:String, ftype:FieldType, nullable:Boolean=true, description:String=null, default:String=null, size:Option[Int] = None, format:String=null) : Field = {
        val field = new Field()
        field._name = name
        field._type = ftype
        field._nullable = nullable.toString
        field._description = description
        field._default = default
        field._format = format
        field._size = size.map(_.toString).orNull
        field
    }
}

/**
  * A Field represents a single entry in a Schema or a Struct.
  */
class Field {
    @JsonProperty(value="name", required = true) private var _name: String = _
    @JsonProperty(value="type", required = false) private var _type: FieldType = _
    @JsonProperty(value="nullable", required = true) private var _nullable: String = "true"
    @JsonProperty(value="description", required = false) private var _description: String = _
    @JsonProperty(value="default", required = false) private var _default: String = _
    @JsonProperty(value="size", required = false) private var _size: String = _
    @JsonProperty(value="format", required = false) private var _format: String = _

    /**
      * The name of the field
      * @return
      */
    def name : String = _name

    /**
      * The type of the field
      * @return
      */
    def ftype : FieldType = _type

    /**
      * Returns true if the field is nullable
      * @return
      */
    def nullable : Boolean = _nullable.toBoolean

    /**
      * Returns an optional description. If there is no description, null will be returned
      * @return
      */
    def description : String = _description

    /**
      * Returns the size of the field. The size is an optional parameter used in fixed-width file formats. It is
      * therefore complementary to any size specification in data types (like char, varchar or decimal). If no
      * size is specified, 0 is returned
      * @return
      */
    def size : Int = Option(_size).map(_.trim).filter(_.nonEmpty).map(_.toInt).getOrElse(0)

    /**
      * Returns an optional default value as a string. If no default value is specified, null is returned instead.
      * @return
      */
    def default : String = _default

    /**
      * Returns an optional format specification as a string. The format specification can be used by relations or
      * file formats, for example to specify the date and time formats
      * @return
      */
    def format : String = _format

    /**
      * Returns an appropriate (Hive) SQL type for this field. These can be directly used in CREATE TABLE statements.
      * The SQL type might also be complex, for example in the case of StructTypes
      * @return
      */
    def sqlType : String = _type.sqlType

    /**
      * Returns an appropriate type name to be used for pretty printing the schema as a tree. Struct types will not
      * be resolved
      * @return
      */
    def typeName : String = _type.typeName

    /**
      * Returns the Spark data type used for this field
      * @return
      */
    def sparkType : DataType = _type.sparkType

    /**
      * Converts the field into a Spark field including metadata containing the fields description and size
      * @return
      */
    def sparkField : StructField = {
        val metadata = new MetadataBuilder()
        Option(description).map(_.trim).filter(_.nonEmpty).foreach(d => metadata.putString("comment", d))
        Option(default).foreach(d => metadata.putString("default", d))
        Option(format).filter(_.nonEmpty).foreach(f => metadata.putString("format", f))
        if (size > 0) metadata.putLong("size", size)
        StructField(name, sparkType, nullable, metadata.build())
    }
}
