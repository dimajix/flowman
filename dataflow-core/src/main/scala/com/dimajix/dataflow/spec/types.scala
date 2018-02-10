package com.dimajix.dataflow.spec

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.node.IntNode
import com.fasterxml.jackson.databind.node.JsonNodeType


object TableIdentifier {
    def apply(name:String, project:String) = new TableIdentifier(name, Some(project))
    def parse(fqName:String) : TableIdentifier= {
        new TableIdentifier(fqName.split('/')(0), None)
    }
}
case class TableIdentifier(name:String, project:Option[String]) {
    override def toString : String = {
        if (project.isEmpty)
            name
        else
            name + "/" + project.get
    }
}


object ConnectionIdentifier {
    def parse(fqName:String) : ConnectionIdentifier = {
        new ConnectionIdentifier(fqName.split('/')(0), None)
    }
}
case class ConnectionIdentifier(name:String, project:Option[String]) {
    override def toString : String = {
        if (project.isEmpty)
            name
        else
            name + "/" + project.get
    }
}


object RelationIdentifier {
    def parse(fqName:String) : RelationIdentifier = {
        new RelationIdentifier(fqName.split('/')(0), None)
    }
}
case class RelationIdentifier(name:String, project:Option[String]) {
    override def toString : String = {
        if (project.isEmpty)
            name
        else
            name + "/" + project.get
    }
}


object OutputIdentifier {
    def parse(fqName:String) : OutputIdentifier = {
        new OutputIdentifier(fqName.split('/')(0), None)
    }
}
case class OutputIdentifier(name:String, project:Option[String]) {
    override def toString : String = {
        if (project.isEmpty)
            name
        else
            name + "/" + project.get
    }
}


private class ValueOrRangeDeserializer(vc:Class[_]) extends StdDeserializer[ValueOrRange](vc) {
    import java.io.IOException

    def this() = this(null)

    @throws[IOException]
    @throws[JsonProcessingException]
    def deserialize(jp: JsonParser, ctxt: DeserializationContext): ValueOrRange = {
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

@JsonDeserialize(using=classOf[ValueOrRangeDeserializer])
class ValueOrRange

case class SingleValue(value:String) extends ValueOrRange { }
case class ArrayValue(value:Array[String]) extends ValueOrRange { }
case class RangeValue(start:String, end:String) extends ValueOrRange { }
