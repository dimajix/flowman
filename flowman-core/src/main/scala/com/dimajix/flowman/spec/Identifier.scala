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

import scala.sys.process.processInternal.URL

import org.apache.hadoop.fs.Path

class IdentifierFactory[T] {
    val empty = new Identifier[T]("", None)

    def apply(name:String) : Identifier[T] = parse(name)
    def apply(name:String, project:Option[String]) = new Identifier[T](name, project)
    def apply(name:String, project:String) = new Identifier[T](name, Some(project))
    def parse(fqName:String) : Identifier[T] = {
        if (fqName == null || fqName.isEmpty) {
            empty
        }
        else {
            val parts = fqName.split('/')
            new Identifier[T](parts.last, if (parts.size > 1) Some(parts.dropRight(1).mkString("/")) else None)
        }
    }
}
case class Identifier[T](name:String, project:Option[String]) {
    def isEmpty : Boolean = name.isEmpty
    def nonEmpty : Boolean = name.nonEmpty

    override def toString : String = {
        if (project.isEmpty)
            name
        else
            project.get + "/" + name
    }
}



object MappingOutputIdentifier {
    val empty = MappingOutputIdentifier("", "", None)
    def apply(name:String) : MappingOutputIdentifier = parse(name)
    def apply(mapping: MappingIdentifier, output:String) : MappingOutputIdentifier = MappingOutputIdentifier(mapping.name, output, mapping.project)

    def parse(fqName:String) : MappingOutputIdentifier = {
        if (fqName == null || fqName.isEmpty) {
            empty
        }
        else {
            val projectTailParts = fqName.split('/')
            val mappingOutput = projectTailParts.last
            val mappingOutputParts = mappingOutput.split(':')
            val project = if (projectTailParts.size > 1) Some(projectTailParts.dropRight(1).mkString("/")) else None
            val mapping = mappingOutputParts.head
            val output = if (mappingOutputParts.size > 1) mappingOutputParts(1) else "main"
            MappingOutputIdentifier(mapping, output, project)
        }
    }
}

case class MappingOutputIdentifier(name:String, output:String, project:Option[String]) {
    def isEmpty : Boolean = name.isEmpty
    def nonEmpty : Boolean = name.nonEmpty

    def mapping : MappingIdentifier = MappingIdentifier(name, project)

    override def toString : String = {
        if (project.isEmpty)
            name + ":" + output
        else
            project.get + "/" + name + ":" + output
    }
}


object ResourceIdentifier {
    def ofFile(file:Path) = ResourceIdentifier("file", file.toString)
    def ofLocal(file:Path) = ResourceIdentifier("local", file.toString)
    def ofHiveDatabase(database:String) = ResourceIdentifier("hiveDatabase", database)
    def ofHiveTable(table:String, database:Option[String]) = ResourceIdentifier("hiveTable", database.map(_ + ".").getOrElse("") + table)
    def ofHivePartition(table:String, database:Option[String], partition:Map[String,Any]) = ResourceIdentifier("hiveTablePartition", database.map(_ + ".").getOrElse("") + table, partition.map { case(k,v) => k -> v.toString })
    def ofURL(url:URL) = ResourceIdentifier("url", url.toString)
}

case class ResourceIdentifier(category:String, name:String, partition:Map[String,String] = Map()) {
    def isEmpty : Boolean = name.isEmpty
    def nonEmpty : Boolean = name.nonEmpty
}
