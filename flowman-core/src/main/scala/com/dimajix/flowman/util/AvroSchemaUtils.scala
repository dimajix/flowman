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

package com.dimajix.flowman.util

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

import com.dimajix.flowman.spec.schema.ArrayType
import com.dimajix.flowman.spec.schema.BinaryType
import com.dimajix.flowman.spec.schema.BooleanType
import com.dimajix.flowman.spec.schema.CharType
import com.dimajix.flowman.spec.schema.DateType
import com.dimajix.flowman.spec.schema.DecimalType
import com.dimajix.flowman.spec.schema.DoubleType
import com.dimajix.flowman.spec.schema.Field
import com.dimajix.flowman.spec.schema.FieldType
import com.dimajix.flowman.spec.schema.FloatType
import com.dimajix.flowman.spec.schema.IntegerType
import com.dimajix.flowman.spec.schema.LongType
import com.dimajix.flowman.spec.schema.MapType
import com.dimajix.flowman.spec.schema.NullType
import com.dimajix.flowman.spec.schema.ShortType
import com.dimajix.flowman.spec.schema.StringType
import com.dimajix.flowman.spec.schema.StructType
import com.dimajix.flowman.spec.schema.TimestampType
import com.dimajix.flowman.spec.schema.VarcharType


object AvroSchemaUtils {
    /**
      * Convert a list of Flowman fields to an Avro (record) schema
      * @param schema
      * @return
      */
    def toAvro(schema:Seq[Field]) : ASchema = {
        ASchema.createRecord("topLevelRecord", null, "", false, schema.map(toAvro))
    }
    def toAvro(field:Field) : AField = toAvro(field, "")
    def toAvro(field:Field, ns:String) : AField = {
        val schema = toAvro(field.ftype, ns, field.name, field.nullable)
        val default = null // if (field.nullable) NULL else null
        new AField(field.name, schema, field.description, default)
    }
    def toAvro(ftype:FieldType, ns:String, name:String, nullable:Boolean) : ASchema = {
        val atype = ftype match {
            case ArrayType(elementType, containsNull) => ASchema.createArray(toAvro(elementType, ns, name, containsNull))
            case BinaryType => ASchema.create(BYTES)
            case BooleanType => ASchema.create(BOOLEAN)
            case CharType(n) => ASchema.create(STRING)
            case VarcharType(n) => ASchema.create(STRING)
            case DoubleType => ASchema.create(DOUBLE)
            case FloatType => ASchema.create(FLOAT)
            case IntegerType => ASchema.create(INT)
            case LongType => ASchema.create(LONG)
            case MapType(keyType, valueType, containsNull) => {
                if (keyType != StringType)
                    throw new IllegalArgumentException("Only strings are supported as keys in Avro maps")
                ASchema.createMap(toAvro(valueType, ns, name, containsNull))
            }
            case NullType => ASchema.create(NULL)
            case ShortType => ASchema.create(INT)
            case StringType => ASchema.create(STRING)
            case StructType(fields) => {
                val nestedNs = ns + "." + name
                ASchema.createRecord(name, null, nestedNs, false, fields.map(f => toAvro(f, nestedNs)))
            }

            //case DurationType =>
            case TimestampType => ASchema.create(LONG)
            case DateType => ASchema.create(LONG)
            case DecimalType(p,s) => ASchema.create(STRING)
            //case ByteType =>
            case _ => throw new IllegalArgumentException(s"Type $ftype not supported in Avro schema")
        }

        if (nullable)
            ASchema.createUnion(atype, ASchema.create(NULL))
        else
            atype
    }

    /**
      * Convert an Avro (record) schema to a list of Flowman fields
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
