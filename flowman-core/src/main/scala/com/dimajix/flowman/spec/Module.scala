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

import java.io.File

import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory

import com.dimajix.flowman.spec.flow.Mapping
import com.dimajix.flowman.spec.model.Relation
import com.dimajix.flowman.spec.output.Output
import com.dimajix.flowman.spec.task.Job
import com.dimajix.flowman.util.splitSettings


object Module {
    class Reader {
        private val logger = LoggerFactory.getLogger(classOf[Project])

        private def loadFile(file:File) : Module = {
            logger.info(s"Reading module file ${file.toString}")
            ObjectMapper.read[Module](file)
        }

        /**
          * Loads a single file or a whole directory (non recursibely)
          *
          * @param file
          * @return
          */
        def file(file:File) : Module = {
            if (file.isDirectory) {
                logger.info(s"Reading all module files in directory ${file.toString}")
                file.listFiles()
                    .filter(_.isFile)
                    .map(f => loadFile(f))
                    .reduce((l,r) => l.merge(r))
            }
            else {
                loadFile(file)
            }
        }
        /**
          * Loads a single file or a whole directory (non recursibely)
          *
          * @param filename
          * @return
          */
        def file(filename:String) : Module = {
            file(new File(filename))
        }

        def string(text:String) : Module = {
            ObjectMapper.parse[Module](text)
        }
    }

    def read = new Reader
}


class Module {
    @JsonProperty(value="environment") private var _environment: Seq[String] = Seq()
    @JsonProperty(value="config") private var _config: Seq[String] = Seq()
    @JsonProperty(value="profiles") private var _profiles: Map[String,Profile] = Map()
    @JsonProperty(value="connections") private var _connections: Map[String,Connection] = Map()
    @JsonProperty(value="relations") private var _relations: Map[String,Relation] = Map()
    @JsonProperty(value="mappings") private var _mappings: Map[String,Mapping] = Map()
    @JsonProperty(value="outputs") private var _outputs: Map[String,Output] = Map()
    @JsonProperty(value="jobs") private var _jobs: Map[String,Job] = Map()

    def profiles : Map[String,Profile] = _profiles
    def relations : Map[String,Relation] = _relations
    def connections : Map[String,Connection] = _connections
    def mappings : Map[String,Mapping] = _mappings
    def outputs : Map[String,Output] = _outputs
    def jobs : Map[String,Job] = _jobs

    /**
      * Returns all configuration variables as a key-value sequence
      *
      * @return
      */
    def config : Seq[(String,String)] = splitSettings(_config)

    /**
      * Returns the environment as a key-value-sequence
      *
      * @return
      */
    def environment : Seq[(String,String)] = splitSettings(_environment)

    /**
      * Creates a new dataflow by merging this one with another one.
      *
      * @param other
      * @return
      */
    def merge(other:Module) : Module = {
        val result = new Module
        result._environment = _environment ++ other._environment
        result._config = _config ++ other._config
        result._connections = _connections ++ other._connections
        result._relations = _relations ++ other._relations
        result._mappings = _mappings ++ other._mappings
        result._outputs = _outputs ++ other._outputs
        result._profiles = _profiles ++ other._profiles
        result._jobs = _jobs ++ other._jobs
        result
    }

    /**
      * Convert this module into a project. This is useful if a module is loaded instead of a project.
      *
      * @param projectName
      * @return
      */
    def toProject(projectName:String) : Project = {
        Project.builder()
                .setName(projectName)
                .setEnvironment(environment)
                .setConfig(config)
                .setProfiles(profiles)
                .setConnections(connections)
                .setRelations(relations)
                .setMappings(mappings)
                .setOutputs(outputs)
                .setJobs(jobs)
                .build()
    }
}
