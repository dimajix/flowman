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
