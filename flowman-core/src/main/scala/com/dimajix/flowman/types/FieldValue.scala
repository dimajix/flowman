/*
 * Copyright (C) 2018 The Flowman Authors
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

import java.sql.Timestamp
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import com.dimajix.flowman.util.UtcTimestamp


private class FieldValueDeserializer(vc:Class[_]) extends StdDeserializer[FieldValue](vc) {
    import java.io.IOException

    def this() = this(null)

    @throws[IOException]
    @throws[JsonProcessingException]
    def deserialize(jp: JsonParser, ctxt: DeserializationContext): FieldValue = {
        val node = jp.getCodec.readTree[JsonNode](jp)
        node.getNodeType match {
            case JsonNodeType.BOOLEAN|JsonNodeType.NUMBER|JsonNodeType.STRING => {
                SingleValue(node.asText)
            }
            case JsonNodeType.ARRAY => {
                val values = 0 until node.size map(node.get(_).asText)
                ArrayValue(values)
            }
            case JsonNodeType.OBJECT => {
                val start = Option(node.get("start")).map(_.asText)
                    .getOrElse(throw JsonMappingException.from(jp, "Missing 'start' value for RangeValue"))
                val end = Option(node.get("end")).map(_.asText)
                    .getOrElse(throw JsonMappingException.from(jp, "Missing 'end' value for RangeValue"))
                val step = Option(node.get("step")).map(_.asText)
                RangeValue(start, end, step)
            }
            case _ => throw JsonMappingException.from(jp, "Wrong type for value/range")
        }
    }
}


object FieldValue {
    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm[:ss]")

    def asString(value:Any) : String = {
        value match {
            case ts: UtcTimestamp => formatter.format(ts.dt)
            case ts: Timestamp => formatter.format(ts.toInstant.atOffset(ZoneOffset.UTC))
            case x => x.toString
        }
    }
    def asLiteral(value:Any) : Column = {
        value match {
            case v: UtcTimestamp => lit(v.toTimestamp())
            case _ => lit(value)
        }
    }
}

@JsonDeserialize(using=classOf[FieldValueDeserializer])
@JsonSchemaInject(
  merge = false,
  json = """
      {
        "type": [ "object", "array", "string", "integer", "boolean" ],
        "anyOf" : [
          {
             "type" : ["string", "integer", "boolean"]
          },
          {
             "type" : "array",
             "items" : {
               "type" : ["string", "integer", "boolean"]
             }
          },
          {
             "type" : "object",
             "properties" : {
                "start" : { "type" : ["string", "integer", "boolean"] },
                "end" : { "type" : ["string", "integer", "boolean"] },
                "step" : { "type" : ["string", "integer"] }
             }
          }
        ]
      }
    """
)
sealed abstract class FieldValue
case class SingleValue(value:String) extends FieldValue {
    require(value != null)
}
case class ArrayValue(values:Seq[String]) extends FieldValue {
    require(values != null)
}
case class RangeValue(start:String, end:String, step:Option[String]=None) extends FieldValue {
    require(start != null)
    require(end != null)
}

object ArrayValue {
    def apply(first: String) : ArrayValue = ArrayValue(Seq(first))
    def apply(first: String, values:String*) : ArrayValue = ArrayValue(first +: values)
}
