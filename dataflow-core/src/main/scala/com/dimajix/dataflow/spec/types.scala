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
