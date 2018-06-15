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


object MappingIdentifier {
    def apply(name:String) = parse(name)
    def apply(name:String, project:String) = new MappingIdentifier(name, Some(project))
    def parse(fqName:String) : MappingIdentifier= {
        val parts = fqName.split('/')
        new MappingIdentifier(parts.last, if (parts.size > 1) Some(parts.dropRight(1).mkString("/")) else None)
    }
}
case class MappingIdentifier(name:String, project:Option[String]) {
    override def toString : String = {
        if (project.isEmpty)
            name
        else
            project.get + "/" + name
    }
}


object ConnectionIdentifier {
    def apply(fqName:String) = parse(fqName)
    def parse(fqName:String) : ConnectionIdentifier = {
        val parts = fqName.split('/')
        new ConnectionIdentifier(parts.last, if (parts.size > 1) Some(parts.dropRight(1).mkString("/")) else None)
    }
}
case class ConnectionIdentifier(name:String, project:Option[String]) {
    override def toString : String = {
        if (project.isEmpty)
            name
        else
            project.get + "/" + name
    }
}


object RelationIdentifier {
    def apply(fqName:String) = parse(fqName)
    def parse(fqName:String) : RelationIdentifier = {
        val parts = fqName.split('/')
        new RelationIdentifier(parts.last, if (parts.size > 1) Some(parts.dropRight(1).mkString("/")) else None)
    }
}
case class RelationIdentifier(name:String, project:Option[String]) {
    override def toString : String = {
        if (project.isEmpty)
            name
        else
            project.get + "/" + name
    }
}


object OutputIdentifier {
    def parse(fqName:String) : OutputIdentifier = {
        val parts = fqName.split('/')
        new OutputIdentifier(parts.last, if (parts.size > 1) Some(parts.dropRight(1).mkString("/")) else None)
    }
}
case class OutputIdentifier(name:String, project:Option[String]) {
    override def toString : String = {
        if (project.isEmpty)
            name
        else
            project.get + "/" + name
    }
}


object JobIdentifier {
    def parse(fqName:String) : JobIdentifier = {
        val parts = fqName.split('/')
        new JobIdentifier(parts.last, if (parts.size > 1) Some(parts.dropRight(1).mkString("/")) else None)
    }
}
case class JobIdentifier(name:String, project:Option[String]) {
    override def toString : String = {
        if (project.isEmpty)
            name
        else
            project.get + "/" + name
    }
}
