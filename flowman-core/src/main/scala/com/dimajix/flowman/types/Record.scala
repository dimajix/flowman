/*
 * Copyright 2021 Kaya Kupferschmidt
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

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.node.JsonNodeType


private class RecordDeserializer(vc:Class[_]) extends StdDeserializer[Record](vc) {
    import java.io.IOException

    def this() = this(null)

    @throws[IOException]
    @throws[JsonProcessingException]
    def deserialize(jp: JsonParser, ctxt: DeserializationContext): Record = {
        val node = jp.getCodec.readTree[JsonNode](jp)
        node.getNodeType match {
            case JsonNodeType.NULL => {
                ValueRecord(null)
            }
            case JsonNodeType.BOOLEAN|JsonNodeType.NUMBER|JsonNodeType.STRING => {
                ValueRecord(node.asText)
            }
            case JsonNodeType.ARRAY => {
                val values = node.iterator().asScala.map { node =>
                    node.getNodeType match {
                        case JsonNodeType.NULL => null
                        case JsonNodeType.BOOLEAN|JsonNodeType.NUMBER|JsonNodeType.STRING => node.asText
                        case _ => throw JsonMappingException.from(jp, "Wrong type for record")
                    }
                }.toList
                ArrayRecord(values)
            }
            case JsonNodeType.OBJECT => {
                val values = node.fields().asScala.map { kv =>
                    kv.getValue.getNodeType match {
                        case JsonNodeType.NULL => kv.getKey -> null
                        case JsonNodeType.BOOLEAN|JsonNodeType.NUMBER|JsonNodeType.STRING => kv.getKey -> kv.getValue.asText
                        case _ => throw JsonMappingException.from(jp, "Wrong type for record")
                    }
                }.toMap
                MapRecord(values)
            }
            case _ => throw JsonMappingException.from(jp, "Wrong type for record")
        }
    }
}


@JsonDeserialize(using=classOf[RecordDeserializer])
sealed abstract class Record {
    def toArray(schema:StructType) : Array[String]
    def map(fn:String => String) : Record
}

final case class ValueRecord(value:String) extends Record {
    override def toArray(schema: StructType): Array[String] = {
        // Append default values
        val tail = schema.fields.tail.map(_.default.orNull).toArray
        Array(value) ++ tail
    }
    override def map(fn:String => String) : ValueRecord = {
        ValueRecord(fn(value))
    }
}

object ArrayRecord {
    def apply(field:String, fields:String*) : ArrayRecord = {
        ArrayRecord(field +: fields)
    }
}
final case class ArrayRecord(fields:Seq[String]) extends Record {
    override def toArray(schema: StructType): Array[String] = {
        if (fields.length >= schema.fields.length) {
            // Either chop off trailing fields
            fields.take(schema.fields.length).toArray
        }
        else {
            // Or append default values
            val tail = schema.fields.drop(fields.length).map(_.default.orNull).toArray
            fields.toArray ++ tail
        }
    }
    override def map(fn:String => String) : ArrayRecord = {
        ArrayRecord(fields.map(fn))
    }
}

object MapRecord {
    def apply(value:(String,String), values:(String,String)*) : MapRecord = {
        MapRecord((value +: values).toMap)
    }
}
final case class MapRecord(values:Map[String,String]) extends Record {
    override def toArray(schema: StructType): Array[String] = {
        schema.fields.map(field => values.getOrElse(field.name, field.default.orNull)).toArray
    }
    override def map(fn:String => String) : MapRecord = {
        MapRecord(values.map(kv => kv._1 -> fn(kv._2)))
    }
}
