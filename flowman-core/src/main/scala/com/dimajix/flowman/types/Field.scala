/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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
    def apply(name:String, ftype:FieldType, nullable:Boolean=true, description:Option[String]=None, default:Option[String]=None, size:Option[Int]=None, format:Option[String]=None) : Field = {
        val field = new Field()
        field._name = name
        field._type = ftype
        field._nullable = nullable
        field._description = description
        field._default = default
        field._format = format
        field._size = size
        field
    }

    def of(field:org.apache.spark.sql.types.StructField) : Field = {
        val recoveredField = com.dimajix.spark.sql.SchemaUtils.recoverCharVarchar(field)
        val ftype = FieldType.of(recoveredField.dataType)
        val description = field.getComment()
        val size = if (field.metadata.contains("size")) Some(field.metadata.getLong("size").toInt) else None
        val default = if (field.metadata.contains("default")) Some(field.metadata.getString("default")) else None
        val format = if (field.metadata.contains("format")) Some(field.metadata.getString("format")) else None
        Field(field.name, ftype, field.nullable, description, default, size, format)
    }

    def of(schema:org.apache.spark.sql.types.StructType) : Seq[Field] = {
        of(schema.fields)
    }

    def of(fields:Seq[org.apache.spark.sql.types.StructField]) : Seq[Field] = {
        fields.map(Field.of)
    }
}

/**
  * A Field represents a single entry in a Schema or a Struct.
  */
final class Field {
    @JsonProperty(value="name", required = true) private var _name: String = _
    @JsonProperty(value="type", required = false) private var _type: FieldType = StringType
    @JsonProperty(value="nullable", required = false) private var _nullable: Boolean = true
    @JsonProperty(value="description", required = false) private var _description: Option[String] = None
    @JsonProperty(value="default", required = false) private var _default: Option[String] = None
    @JsonProperty(value="size", required = false) private var _size: Option[Int] = None
    @JsonProperty(value="format", required = false) private var _format: Option[String] = None

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
    def nullable : Boolean = _nullable

    /**
      * Returns an optional description. If there is no description, null will be returned
      * @return
      */
    def description : Option[String] = _description

    /**
      * Returns the size of the field. The size is an optional parameter used in fixed-width file formats. It is
      * therefore complementary to any size specification in data types (like char, varchar or decimal).
      * @return
      */
    def size : Option[Int] = _size

    /**
      * Returns an optional default value as a string.
      * @return
      */
    def default : Option[String] = _default

    /**
      * Returns an optional format specification as a string. The format specification can be used by relations or
      * file formats, for example to specify the date and time formats
      * @return
      */
    def format : Option[String] = _format

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
     * Returns the Spark data type used for this field in catalog definitions (i.e. Hive)
     * @return
     */
    def catalogType : DataType = _type.catalogType

    /**
      * Converts the field into a Spark field including metadata containing the fields description and size
      * @return
      */
    def sparkField : StructField = {
        com.dimajix.spark.sql.SchemaUtils.replaceCharVarchar(catalogField)
    }

    /**
     * Converts the field into a Spark field including metadata containing the fields description and size. This
     * method is to be used for creating a schema for external catalogs (Hive)
     * @return
     */
    def catalogField : StructField = {
        StructField(name, catalogType, nullable, metadata)
    }

    private def metadata = {
        val metadata = new MetadataBuilder()
        description.map(_.trim).filter(_.nonEmpty).foreach(d => metadata.putString("comment", d))
        default.foreach(d => metadata.putString("default", d))
        format.filter(_.nonEmpty).foreach(f => metadata.putString("format", f))
        size.foreach(s => metadata.putLong("size", s))
        metadata.build()
    }

    override def toString: String = {
        val format = this.format.map(", format=" + _).getOrElse("")
        val default = this.default.map(", default=" + _).getOrElse("")
        val size = this.size.map(", size=" + _).getOrElse("")
        val desc = this.description.map(", description=\"" + _ + "\"").getOrElse("")
        s"Field($name, $ftype, $nullable$format$size$default$desc))"
    }


    def canEqual(other: Any): Boolean = other.isInstanceOf[Field]

    override def equals(other: Any): Boolean = other match {
        case that: Field =>
            (that canEqual this) &&
                _name == that._name &&
                _type == that._type &&
                _nullable == that._nullable &&
                _description == that._description &&
                _default == that._default &&
                _size == that._size &&
                _format == that._format
        case _ => false
    }

    override def hashCode(): Int = {
        val state = Seq(_name, _type, _nullable, _description, _default, _size, _format)
        state.map(o => if (o != null) o.hashCode() else 0).foldLeft(0)((a, b) => 31 * a + b)
    }

    def copy(name:String=_name,
             ftype:FieldType=_type,
             nullable:Boolean=_nullable,
             description:Option[String]=_description,
             default:Option[String]=_default,
             size:Option[Int]=_size,
             format:Option[String]=_format) : Field = {
        Field(name, ftype, nullable, description, default, size, format)
    }
}
