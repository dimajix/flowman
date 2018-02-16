package com.dimajix.flowman.spec.schema

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
                val start = node.get("start").asText
                val end = node.get("end").asText
                RangeValue(start, end)
            }
            case _ => throw new JsonMappingException(jp, "Wrong type for value/range")
        }
    }
}

@JsonDeserialize(using=classOf[FieldValueDeserializer])
class FieldValue
case class SingleValue(value:String) extends FieldValue { }
case class ArrayValue(values:Array[String]) extends FieldValue { }
case class RangeValue(start:String, end:String) extends FieldValue { }
