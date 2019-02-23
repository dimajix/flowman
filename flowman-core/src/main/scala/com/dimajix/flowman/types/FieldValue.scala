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

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.node.JsonNodeType


private class FieldValueDeserializer(vc:Class[_]) extends StdDeserializer[FieldValue](vc) {
    import java.io.IOException

    def this() = this(null)

    @throws[IOException]
    @throws[JsonProcessingException]
    def deserialize(jp: JsonParser, ctxt: DeserializationContext): FieldValue = {
        val node = jp.getCodec.readTree[JsonNode](jp)
        node.getNodeType match {
            case JsonNodeType.STRING => {
                SingleValue(node.asText)
            }
            case JsonNodeType.ARRAY => {
                val values = 0 until node.size map(node.get(_).asText)
                ArrayValue(values.toArray)
            }
            case JsonNodeType.OBJECT => {
                val start = node.get("startJob").asText
                val end = node.get("end").asText
                val step = Option(node.get("step")).map(_.asText).orNull
                RangeValue(start, end, step)
            }
            case _ => throw JsonMappingException.from(jp, "Wrong type for value/range")
        }
    }
}

@JsonDeserialize(using=classOf[FieldValueDeserializer])
class FieldValue
case class SingleValue(value:String) extends FieldValue { }
case class ArrayValue(values:Array[String]) extends FieldValue { }
case class RangeValue(start:String, end:String, step:String=null) extends FieldValue { }

object ArrayValue {
    def apply(values:String*) : ArrayValue = ArrayValue(values.toArray)
}
