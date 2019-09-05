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

import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory

import com.dimajix.flowman.hadoop.File
import com.dimajix.flowman.spec.connection.ConnectionSpec
import com.dimajix.flowman.spec.flow.MappingSpec
import com.dimajix.flowman.spec.model.RelationSpec
import com.dimajix.flowman.spec.target.BundleSpec
import com.dimajix.flowman.spec.target.TargetSpec


object Project {
    class Reader {
        private val logger = LoggerFactory.getLogger(classOf[Reader])

        /**
          * Loads a project file and all related module files
          *
          * @param file
          * @return
          */
        def file(file:File) : Project = {
            if (!file.isAbsolute()) {
                readFile(file.absolute)
            }
            else {
                readFile(file)
            }
        }

        /**
          * Read in only main definition file without additonal modules
          * @param file
          * @return
          */
        def manifest(file:File) : Project = {
            if (!file.isAbsolute()) {
                manifest(file.absolute)
            }
            else if (file.isDirectory) {
                logger.info(s"Reading project manifest in directory $file")
                manifest(file / "project.yml")
            }
            else {
                logger.info(s"Reading project manifest from $file")
                val project = ObjectMapper.read[Project](file)
                project._filename = file.absolute
                project._basedir = file.absolute.parent
                project
            }
        }

        def string(text:String) : Project = {
            ObjectMapper.parse[Project](text)
        }

        private def readFile(file:File) : Project = {
            if (file.isDirectory) {
                logger.info(s"Reading project in directory $file")
                this.file(file / "project.yml")
            }
            else {
                logger.info(s"Reading project from $file")
                val project = ObjectMapper.read[Project](file)
                loadModules(project, file.parent)
                project._filename = file.absolute
                project._basedir = file.absolute.parent
                project
            }
        }

        private def loadModules(project: Project, directory:File) : Unit = {
            val module = project.modules
                .map(f => Module.read.file(directory / f))
                .reduce((l,r) => l.merge(r))

            project._environment = module.environment
            project._config = module.config
            project._profiles = module.profiles
            project._connections = module.connections
            project._relations = module.relations
            project._mappings = module.mappings
            project._targets = module.targets
            project._bundles = module.bundles
            // TODO: Metrics
        }
    }

    class Builder {
        private val project = new Project

        def build() : Project = project

        def setName(name:String) : Builder = {
            project._name = name
            this
        }
        def setDescription(desc:String) : Builder = {
            project._description = Some(desc)
            this
        }

        def setEnvironment(env:Map[String,String]) : Builder = {
            project._environment = env
            this
        }
        def setConfig(conf:Map[String,String]) : Builder = {
            project._config = conf
            this
        }
        def setProfiles(profiles:Map[String,Profile]) : Builder = {
            project._profiles = profiles
            this
        }
        def addProfile(name:String, profile:Profile) : Builder = {
            project._profiles = project._profiles + (name -> profile)
            this
        }
        def setConnections(connections:Map[String,ConnectionSpec]) : Builder = {
            project._connections = connections
            this
        }
        def addConnection(name:String, connection:ConnectionSpec) : Builder = {
            project._connections = project._connections + (name -> connection)
            this
        }
        def setRelations(relations:Map[String,RelationSpec]) : Builder = {
            project._relations = relations
            this
        }
        def addRelations(name:String, relation:RelationSpec) : Builder = {
            project._relations = project._relations + (name -> relation)
            this
        }
        def setMappings(mappings:Map[String,MappingSpec]) : Builder = {
            project._mappings = mappings
            this
        }
        def addMapping(name:String, mapping:MappingSpec) : Builder = {
            project._mappings = project._mappings + (name -> mapping)
            this
        }
        def setTargets(targets:Map[String,TargetSpec]) : Builder = {
            project._targets = targets
            this
        }
        def addTarget(name:String, target:TargetSpec) : Builder = {
            project._targets = project._targets + (name -> target)
            this
        }
        def setBundles(bundles:Map[String,BundleSpec]) : Builder = {
            project._bundles = bundles
            this
        }
        def addBundle(name:String, bundle:BundleSpec) : Builder = {
            project._bundles = project._bundles + (name -> bundle)
            this
        }
    }

    def read = new Reader

    def builder() = new Builder
}


class Project {
    @JsonProperty(value="name", required = true) private var _name: String = _
    @JsonProperty(value="description", required = false) private var _description: Option[String] = None
    @JsonProperty(value="version", required = false) private var _version: String = _
    @JsonProperty(value="modules", required = true) private var _modules: Seq[String] = Seq()

    private var _basedir: File = File.empty
    private var _filename: File = File.empty

    private var _environment: Map[String,String] = Map()
    private var _config: Map[String,String] = Map()
    private var _profiles: Map[String,Profile] = Map()
    private var _connections: Map[String,ConnectionSpec] = Map()
    private var _relations: Map[String,RelationSpec] = Map()
    private var _mappings: Map[String,MappingSpec] = Map()
    private var _targets: Map[String,TargetSpec] = Map()
    private var _bundles: Map[String,BundleSpec] = Map()

    def name : String = _name
    def description : Option[String] = _description
    def version : String = _version
    def modules : Seq[String] = _modules
    def filename : File = _filename
    def basedir : File = _basedir

    def config : Map[String,String] = _config
    def environment : Map[String,String] = _environment

    def profiles : Map[String,Profile] = _profiles
    def relations : Map[String,RelationSpec] = _relations
    def connections : Map[String,ConnectionSpec] = _connections
    def mappings : Map[String,MappingSpec] = _mappings
    def targets : Map[String,TargetSpec] = _targets
    def bundles : Map[String,BundleSpec] = _bundles
}
