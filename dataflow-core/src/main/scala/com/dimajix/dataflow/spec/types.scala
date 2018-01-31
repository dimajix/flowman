package com.dimajix.dataflow.spec


object TableIdentifier {
    def apply(name:String, project:String) = new TableIdentifier(name, Some(project))
    def parse(fqName:String) : TableIdentifier= {
        new TableIdentifier(fqName.split('/')(0), None)
    }
}
case class TableIdentifier(name:String, project:Option[String]) {
}


object ConnectionIdentifier {
    def parse(fqName:String) : ConnectionIdentifier = {
        new ConnectionIdentifier(fqName.split('/')(0), None)
    }
}
case class ConnectionIdentifier(name:String, project:Option[String]) {
}

object RelationIdentifier {
    def parse(fqName:String) : RelationIdentifier = {
        new RelationIdentifier(fqName.split('/')(0), None)
    }
}
case class RelationIdentifier(name:String, project:Option[String]) {
}

object OutputIdentifier {
    def parse(fqName:String) : OutputIdentifier = {
        new OutputIdentifier(fqName.split('/')(0), None)
    }
}
case class OutputIdentifier(name:String, project:Option[String]) {
}
