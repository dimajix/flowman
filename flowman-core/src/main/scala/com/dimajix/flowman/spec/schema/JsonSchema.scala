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

import scala.collection.JavaConversions._

import org.everit.json.schema.ArraySchema
import org.everit.json.schema.BooleanSchema
import org.everit.json.schema.EnumSchema
import org.everit.json.schema.NullSchema
import org.everit.json.schema.NumberSchema
import org.everit.json.schema.ObjectSchema
import org.everit.json.schema.StringSchema
import org.everit.json.schema.loader.SchemaLoader
import org.everit.json.schema.{Schema => JSchema}
import org.json.JSONObject
import org.json.JSONTokener

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.schema.ExternalSchema.CachedSchema
import com.dimajix.flowman.types.ArrayType
import com.dimajix.flowman.types.BooleanType
import com.dimajix.flowman.types.DoubleType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.LongType
import com.dimajix.flowman.types.NullType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.types.VarcharType

/**
  * This class encapsulates a data frame schema specified as a JSON schema document.
  */
class JsonSchema extends ExternalSchema {
    /**
      * Returns the description of the whole schema
      * @param context
      * @return
      */
    protected override def loadSchema(implicit context: Context): CachedSchema = {
        val spec = loadSchemaSpec

        val rawSchema = new JSONObject(new JSONTokener(spec))
        val jsonSchema = SchemaLoader.load(rawSchema)
        if (!jsonSchema.isInstanceOf[ObjectSchema])
            throw new UnsupportedOperationException("Unexpected JSON top level type")

        CachedSchema(
            fromJsonObject(jsonSchema.asInstanceOf[ObjectSchema]).fields,
            jsonSchema.getDescription
        )
    }

    private def fromJsonObject(obj:ObjectSchema) : StructType = {
        val requiredProperties = obj.getRequiredProperties.toSet
        val fields = obj.getPropertySchemas.toSeq.sortBy(_._1)
        StructType(fields.map(nt => fromJsonField(nt._1, nt._2, requiredProperties.contains(nt._1))))
    }

    private def fromJsonField(name:String, schema:JSchema, required: Boolean) : Field = {
        Field(name, fromJsonType(schema), !required && Option(schema.isNullable).forall(_.booleanValue()), schema.getDescription)
    }

    private def fromJsonType(schema:JSchema) : FieldType = {
        schema match {
            case array:ArraySchema => ArrayType(fromJsonType(array.getAllItemSchema))
            case _:BooleanSchema => BooleanType
            case _:EnumSchema => StringType
            case _:NullSchema => NullType
            case number:NumberSchema => if (number.requiresInteger()) {
                LongType
            }
            else {
                DoubleType
            }
            case obj:ObjectSchema => fromJsonObject(obj)
            case string:StringSchema => {
                if (string.getMaxLength != null && string.getMaxLength < Integer.MAX_VALUE)
                    VarcharType(string.getMaxLength)
                else
                    StringType
            }
            case _ => throw new UnsupportedOperationException(s"Unsupported type in JSON schema")
        }
    }
}
