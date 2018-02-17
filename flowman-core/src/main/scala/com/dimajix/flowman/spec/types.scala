package com.dimajix.flowman.spec


object TableIdentifier {
    def apply(name:String) = parse(name)
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
    def apply(fqName:String) = parse(fqName)
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
    def apply(fqName:String) = parse(fqName)
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
