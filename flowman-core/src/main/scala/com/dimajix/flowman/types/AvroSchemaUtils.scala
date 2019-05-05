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

import scala.collection.JavaConversions._

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
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.node.BooleanNode
import org.codehaus.jackson.node.DoubleNode
import org.codehaus.jackson.node.IntNode
import org.codehaus.jackson.node.LongNode
import org.codehaus.jackson.node.NullNode
import org.codehaus.jackson.node.TextNode


object AvroSchemaUtils {
    /**
      * Convert a list of Flowman fields to an Avro (record) schema. Note that this logic should be compatible
      * to the Spark-Avro implementation!
      * @param schema
      * @return
      */
    def toAvro(schema:Seq[Field]) : ASchema = {
        val record = ASchema.createRecord("topLevelRecord", null, "", false)
        record.setFields(schema.map(toAvro))
        record
    }
    def toAvro(field:Field) : AField = {
        toAvro(field, "")
    }
    private def toAvro(field:Field, ns:String) : AField = {
        val schema = toAvro(field.ftype, ns, field.name, field.nullable)
        val default = toAvroDefault(field)
        new AField(field.name, schema, field.description, default:Object)
    }
    private def toAvro(ftype:FieldType, ns:String, name:String, nullable:Boolean) : ASchema = {
        val atype = ftype match {
            case ArrayType(elementType, containsNull) => ASchema.createArray(toAvro(elementType, ns, name, containsNull))
            case BinaryType => ASchema.create(BYTES)
            case BooleanType => ASchema.create(BOOLEAN)
            case CharType(n) => ASchema.create(STRING)
            case VarcharType(n) => ASchema.create(STRING)
            case DoubleType => ASchema.create(DOUBLE)
            case FloatType => ASchema.create(FLOAT)
            case ByteType => ASchema.create(INT)
            case ShortType => ASchema.create(INT)
            case IntegerType => ASchema.create(INT)
            case LongType => ASchema.create(LONG)
            case MapType(keyType, valueType, containsNull) => {
                if (keyType != StringType)
                    throw new IllegalArgumentException("Only strings are supported as keys in Avro maps")
                ASchema.createMap(toAvro(valueType, ns, name, containsNull))
            }
            case NullType => ASchema.create(NULL)
            case StringType => ASchema.create(STRING)
            case StructType(fields) => {
                val nestedNs = ns + "." + name
                val record = ASchema.createRecord(name, null, nestedNs, false)
                record.setFields(fields.map(f => toAvro(f, nestedNs)))
                record
            }

            //case DurationType =>
            case TimestampType => ASchema.create(LONG)
            case DateType => ASchema.create(LONG)
            case DecimalType(p,s) => ASchema.create(STRING)
            case _ => throw new IllegalArgumentException(s"Type $ftype not supported in Avro schema")
        }

        if (nullable)
            ASchema.createUnion(Seq(atype, ASchema.create(NULL)))
        else
            atype
    }
    private def toAvroDefault(field:Field) : JsonNode = {
        if (field.default != null) {
            field.ftype match {
                case StringType => new TextNode(field.default)
                case CharType(_) => new TextNode(field.default)
                case VarcharType(_) => new TextNode(field.default)
                case BinaryType => new TextNode(field.default)
                case IntegerType => new IntNode(field.default.toInt)
                case ByteType => new IntNode(field.default.toInt)
                case ShortType => new IntNode(field.default.toInt)
                case LongType => new LongNode(field.default.toLong)
                case FloatType => new DoubleNode(field.default.toDouble)
                case DoubleType => new DoubleNode(field.default.toDouble)
                case DecimalType(_,_) => new TextNode(field.default)
                case BooleanType => if (field.default.toBoolean) BooleanNode.TRUE else BooleanNode.FALSE
                case NullType => NullNode.instance
                case _ => null
            }
        }
        else {
            null
        }
    }

    /**
      * Convert an Avro (record) schema to a list of Flowman fields. Note that this logic should be
      * compatible to from Spark-Avro implementation!
      * @param schema
      * @return
      */
    def fromAvro(schema: ASchema) : Seq[Field] = {
        if (schema.getType != RECORD)
            throw new UnsupportedOperationException("Unexpected Avro top level type")

        schema.getFields.map(AvroSchemaUtils.fromAvro)
    }

    def fromAvro(field: AField) : Field = {
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

            case other => throw new UnsupportedOperationException(s"Unsupported type $other in Avro schema")
        }
    }

}
