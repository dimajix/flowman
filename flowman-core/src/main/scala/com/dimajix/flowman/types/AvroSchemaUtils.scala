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

import scala.collection.JavaConverters._

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
import org.slf4j.LoggerFactory


class AvroSchemaUtils
object AvroSchemaUtils {
    private val logger = LoggerFactory.getLogger(classOf[AvroSchemaUtils])

    /**
      * Convert a list of Flowman fields to an Avro (record) schema. Note that this logic should be compatible
      * to the Spark-Avro implementation!
      * @param schema
      * @return
      */
    def toAvro(schema:Seq[Field]) : ASchema = {
        val record = ASchema.createRecord("topLevelRecord", null, "", false)
        record.setFields(schema.map(toAvro).asJava)
        record
    }
    def toAvro(field:Field) : AField = {
        toAvro(field, "")
    }
    private def toAvro(field:Field, ns:String) : AField = {
        val schema = toAvro(field.ftype, ns, field.name, field.nullable)
        val default = toAvroDefault(field)
        new AField(field.name, schema, field.description.orNull, default)
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
                record.setFields(fields.map(f => toAvro(f, nestedNs)).asJava)
                record
            }

            //case DurationType =>
            case TimestampType => ASchema.create(LONG)
            case DateType => ASchema.create(INT)
            case DecimalType(p,s) => ASchema.create(STRING)
            case _ => throw new IllegalArgumentException(s"Type $ftype not supported in Avro schema")
        }

        if (nullable)
            ASchema.createUnion(Seq(atype, ASchema.create(NULL)).asJava)
        else
            atype
    }
    private def toAvroDefault(field:Field) : AnyRef = {
        field.default.map  { default =>
            field.ftype match {
                case StringType => default
                case CharType(_) => default
                case VarcharType(_) => default
                case BinaryType => default
                case IntegerType => Integer.valueOf(default.toInt)
                case ByteType => Integer.valueOf(default.toInt)
                case ShortType => Integer.valueOf(default.toInt)
                case LongType => java.lang.Long.valueOf(default.toLong)
                case FloatType => java.lang.Double.valueOf(default.toDouble)
                case DoubleType => java.lang.Double.valueOf(default.toDouble)
                case DecimalType(_,_) => default
                case BooleanType => if (default.toBoolean) java.lang.Boolean.TRUE else java.lang.Boolean.FALSE
                case NullType => null
                case _ => null
            }
        }.orNull
    }

    /**
      * Convert an Avro (record) schema to a list of Flowman fields. Note that this logic should be
      * compatible to from Spark-Avro implementation!
      * @param schema
      * @return
      */
    def fromAvro(schema: ASchema, forceNullable:Boolean=false) : Seq[Field] = {
        if (schema.getType != RECORD)
            throw new UnsupportedOperationException("Unexpected Avro top level type")

        schema.getFields.asScala.map(f => AvroSchemaUtils.fromAvro(f, forceNullable))
    }

    def fromAvro(field: AField, forceNullable:Boolean) : Field = {
        val (ftype,nullable) = fromAvroType(field.schema(), forceNullable)
        Field(field.name(), ftype, nullable, Option(field.doc()))
    }
    private def fromAvroType(schema: ASchema, forceNullable:Boolean): (FieldType,Boolean) = {
        schema.getType match {
            case INT =>
                Option(schema.getProp("logicalType")) match {
                    case Some("date") => (DateType, forceNullable)
                    case None => (IntegerType, forceNullable)
                    case Some(lt) =>
                        logger.warn(s"Avro logical type '$lt' of type 'INT' not supported - simply using INT")
                        (IntegerType, forceNullable)
                }
            case STRING => (StringType, forceNullable)
            case BOOLEAN => (BooleanType, forceNullable)
            case BYTES => (BinaryType, forceNullable)
            case DOUBLE => (DoubleType, forceNullable)
            case FLOAT => (FloatType, forceNullable)
            case LONG =>
                Option(schema.getProp("logicalType")) match {
                    case Some("timestamp-millis") => (TimestampType, forceNullable)
                    case None => (LongType, forceNullable)
                    case Some(lt) =>
                        logger.warn(s"Avro logical type '$lt' of type 'LONG' not supported - simply using LONG")
                        (LongType, forceNullable)
                }
            case FIXED => (BinaryType, forceNullable)
            case ENUM => (StringType, forceNullable)

            case RECORD =>
                val fields = schema.getFields.asScala.map { f =>
                    val (schemaType,nullable) = fromAvroType(f.schema(), forceNullable)
                    Field(f.name, schemaType, nullable, Option(f.doc()))
                }
                (StructType(fields), forceNullable)

            case ARRAY =>
                val (schemaType, nullable) = fromAvroType(schema.getElementType, forceNullable)
                (ArrayType(schemaType, nullable), forceNullable)

            case MAP =>
                val (schemaType, nullable) = fromAvroType(schema.getValueType, forceNullable)
                (MapType(StringType, schemaType, nullable), forceNullable)

            case UNION =>
                if (schema.getTypes.asScala.exists(_.getType == NULL)) {
                    // In case of a union with null, eliminate it and make a recursive call
                    val remainingUnionTypes = schema.getTypes.asScala.filterNot(_.getType == NULL)
                    if (remainingUnionTypes.size == 1) {
                        (fromAvroType(remainingUnionTypes.head, forceNullable)._1, true)
                    } else {
                        (fromAvroType(ASchema.createUnion(remainingUnionTypes.asJava), forceNullable)._1, true)
                    }
                } else schema.getTypes.asScala.map(_.getType) match {
                    case Seq(t1) =>
                        fromAvroType(schema.getTypes.get(0), forceNullable)
                    case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
                        (LongType, forceNullable)
                    case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
                        (DoubleType, forceNullable)
                    case other => throw new UnsupportedOperationException(
                        s"This mix of union types is not supported: $other")
                }

            case other => throw new UnsupportedOperationException(s"Unsupported type $other in Avro schema")
        }
    }

}
