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

package com.dimajix.flowman.namespace.storage

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.flowman.spec.Profile
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.connection.Connection
import com.dimajix.flowman.spec.model.Relation
import com.dimajix.flowman.spi.TypeRegistry


object Store extends TypeRegistry[Store] {
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "file", value = classOf[FileStore])
))
abstract class Store {
    def loadProject(name:String) : Project
    def storeProject(project: Project) : Unit
    def removeProject(name:String) : Unit
    def listProjects() : Seq[String]

    def loadEnvironment() : Map[String,String]
    def addEnvironment(key:String, value:String) : Unit
    def removeEnvironment(key:String) : Unit

    def listProfiles() : Seq[String]
    def loadProfiles() : Map[String,Profile]
    def enableProfile(name:String) : Unit
    def disableProfile(name:String) : Unit
    def loadProfile(name:String) : Profile
    def storeProfile(name:String, profile: Profile) : Unit
    def removeProfile(name:String) : Unit

    def listRelations() : Seq[String]
    def loadRelations() : Map[String,Relation]
    def loadRelation(name:String) : Relation
    def storeRelation(name:String, relation: Relation) : Unit
    def removeRelation(name:String) : Unit

    def listDatabases() : Seq[String]
    def loadDatabases() : Map[String,Connection]
    def loadDatabase(name:String) : Connection
    def storeDatabase(name:String, database: Connection) : Unit
    def removeDatabase(name:String) : Unit
}
