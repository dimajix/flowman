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

package com.dimajix.flowman.spec.schema

import java.io.File
import java.net.URL

import scala.collection.JavaConversions._

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.avro.Schema.Type.ARRAY
import org.apache.avro.Schema.Type.BOOLEAN
import org.apache.avro.Schema.Type.BYTES
import org.apache.avro.Schema.Type.DOUBLE
import org.apache.avro.Schema.Type.ENUM
import org.apache.avro.Schema.Type.FIXED
import org.apache.avro.Schema.Type.FLOAT
import org.apache.avro.Schema.Type.INT
import org.apache.avro.Schema.Type.LONG
import org.apache.avro.Schema.Type.MAP
import org.apache.avro.Schema.Type.NULL
import org.apache.avro.Schema.Type.RECORD
import org.apache.avro.Schema.Type.STRING
import org.apache.avro.Schema.Type.UNION
import org.apache.avro.Schema.{Field => AField}
import org.apache.avro.{Schema => ASchema}

import com.dimajix.flowman.execution.Context


class AvroSchema extends Schema {
    @JsonProperty(value="file", required=false) private var _file: String = _
    @JsonProperty(value="url", required=false) private var _url: String = _
    @JsonProperty(value="spec", required=false) private var _spec: String = _

    def file(implicit context: Context) : String = context.evaluate(_file)
    def url(implicit context: Context) : URL = if (_url != null && _url.nonEmpty) new URL(context.evaluate(_url)) else null
    def spec(implicit context: Context) : String = context.evaluate(_spec)

    override def description(implicit context: Context): String = {
        loadSchema._1
    }
    override def fields(implicit context: Context): Seq[Field] = {
        loadSchema._2
    }

    private def loadSchema(implicit context: Context) : (String, Seq[Field]) = {
        val file = this.file
        val url = this.url
        val spec = this.spec

        val avroSchema = if (file != null && file.nonEmpty) {
            new org.apache.avro.Schema.Parser().parse(new File(file))
        }
        else if (url != null) {
            val con = url.openConnection()
            con.setUseCaches(false)
            new org.apache.avro.Schema.Parser().parse(con.getInputStream)
        }
        else if (spec != null && spec.nonEmpty) {
            new org.apache.avro.Schema.Parser().parse(spec)
        }
        else {
            throw new IllegalArgumentException("An Avro schema needs either a 'file', 'url' or a 'spec' element")
        }

        if (avroSchema.getType != RECORD)
            throw new UnsupportedOperationException("Unexpected Avro top level type")

        (avroSchema.getDoc, avroSchema.getFields.map(fromAvroField))
    }

    private def fromAvroField(field: AField) : Field = {
        val (ftype,nullable) = fromAvroType(field.schema())
        Field(field.name(), ftype, nullable, field.doc())
    }
    private def fromAvroType(schema: ASchema): (FieldType,Boolean) = {
        schema.getType match {
            case INT => (IntegerType, false)
            case STRING => (StringType, false)
            case BOOLEAN => (BooleanType, false)
            case BYTES => (BinaryType, false)
            case DOUBLE => (DoubleType, false)
            case FLOAT => (FloatType, false)
            case LONG => (LongType, false)
            case FIXED => (BinaryType, false)
            case ENUM => (StringType, false)

            case RECORD =>
                val fields = schema.getFields.map { f =>
                    val (schemaType,nullable) = fromAvroType(f.schema())
                    Field(f.name, schemaType, nullable, f.doc())
                }
                (StructType(fields), false)

            case ARRAY =>
                val (schemaType, nullable) = fromAvroType(schema.getElementType)
                (ArrayType(schemaType, nullable), false)

            case MAP =>
                val (schemaType, nullable) = fromAvroType(schema.getValueType)
                (MapType(StringType, schemaType, nullable), false)

            case UNION =>
                if (schema.getTypes.exists(_.getType == NULL)) {
                    // In case of a union with null, eliminate it and make a recursive call
                    val remainingUnionTypes = schema.getTypes.filterNot(_.getType == NULL)
                    if (remainingUnionTypes.size == 1) {
                        (fromAvroType(remainingUnionTypes.get(0))._1, true)
                    } else {
                        (fromAvroType(ASchema.createUnion(remainingUnionTypes))._1, true)
                    }
                } else schema.getTypes.map(_.getType) match {
                    case Seq(t1) =>
                        fromAvroType(schema.getTypes.get(0))
                    case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
                        (LongType, false)
                    case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
                        (DoubleType, false)
                    case other => throw new UnsupportedOperationException(
                        s"This mix of union types is not supported: $other")
                }

            case other => throw new UnsupportedOperationException(s"Unsupported type $other")
        }
    }
}
