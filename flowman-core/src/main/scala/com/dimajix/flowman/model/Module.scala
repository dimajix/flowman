/*
 * Copyright 2018-2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.model

import java.net.URL
import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.slf4j.LoggerFactory

import com.dimajix.flowman.hadoop.File
import com.dimajix.flowman.spi.ModuleReader


object Module {
    private lazy val loader = ServiceLoader.load(classOf[ModuleReader]).iterator().asScala.toSeq

    class Reader {
        private val logger = LoggerFactory.getLogger(classOf[Module])
        private var format = "yaml"

        def format(fmt:String) : Reader = {
            format = fmt
            this
        }

        /**
         * Loads a single file or a whole directory (non recursibely)
         *
         * @param file
         * @return
         */
        def file(file:File) : Module = {
            if (!file.isAbsolute()) {
                readFile(file.absolute)
            }
            else {
                readFile(file)
            }
        }

        def url(url:URL) : Module = {
            logger.info(s"Reading module from url ${url.toString}")
            val stream = url.openStream()
            try {
                reader.stream(stream)
            }
            finally {
                stream.close()
            }
        }

        def string(text:String) : Module = {
            reader.string(text)
        }

        private def readFile(file:File) : Module = {
            if (file.isDirectory) {
                logger.info(s"Reading all module files in directory ${file.toString}")
                file.list()
                    .filter(_.isFile)
                    .map(f => loadFile(f))
                    .foldLeft(Module())((l,r) => l.merge(r))
            }
            else {
                loadFile(file)
            }
        }

        private def loadFile(file:File) : Module = {
            logger.info(s"Reading module from ${file.toString}")
            reader.file(file)
        }

        private def reader : ModuleReader = {
            loader.find(_.supports(format))
                .getOrElse(throw new IllegalArgumentException(s"Module format '$format' not supported'"))
        }
    }

    def read = new Reader
}


final case class Module(
   config : Map[String,String] = Map(),
   environment : Map[String,String] = Map(),

   profiles : Map[String,Profile] = Map(),
   relations : Map[String,Template[Relation]] = Map(),
   connections : Map[String,Template[Connection]] = Map(),
   mappings : Map[String,Template[Mapping]] = Map(),
   targets : Map[String,Template[Target]] = Map(),
   jobs : Map[String,Template[Job]] = Map(),
   tests : Map[String,Template[Test]] = Map()
) {
    /**
     * Creates a new dataflow by merging this one with another one.
     *
     * @param other
     * @return
     */
    def merge(other:Module) : Module = {
        Module(
            config = config ++ other.config,
            environment = environment ++ other.environment,
            profiles = profiles ++ other.profiles,
            relations = relations ++ other.relations,
            connections = connections ++ other.connections,
            mappings = mappings ++ other.mappings,
            targets = targets ++ other.targets,
            jobs = jobs ++ other.jobs,
            tests = tests ++ other.tests
        )
    }

    /**
     * Convert this module into a project. This is useful if a module is loaded instead of a project.
     *
     * @param projectName
     * @return
     */
    def toProject(projectName:String) : Project = {
        Project(
            projectName,
            environment = environment,
            config = config,
            profiles = profiles,
            connections = connections,
            relations = relations,
            mappings = mappings,
            targets = targets,
            jobs = jobs,
            tests = tests
        )
    }
}



