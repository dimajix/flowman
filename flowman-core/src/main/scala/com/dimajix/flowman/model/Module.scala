/*
 * Copyright (C) 2018 The Flowman Authors
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
import scala.util.control.NonFatal

import org.slf4j.LoggerFactory

import com.dimajix.flowman.fs.File
import com.dimajix.flowman.fs.GlobPattern
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
        @throws[ModelException]
        def file(file:File) : Module = {
            files(file)
                .map(_._2)
                .foldLeft(Module())((l, r) => l.merge(r))
        }

        @throws[ModelException]
        def files(file: File): Seq[(File,Module)] = {
            if (!file.isAbsolute()) {
                readFiles(file.absolute)
            }
            else {
                readFiles(file)
            }
        }

        @throws[ModelException]
        def url(url:URL) : Module = {
            logger.info(s"Reading module from url ${url.toString}")
            wrapExceptions(url.toString) {
                val stream = url.openStream()
                try {
                    reader.stream(stream)
                }
                finally {
                    stream.close()
                }
            }
        }

        @throws[ModelException]
        def string(text:String) : Module = {
            wrapExceptions("raw-string") {
                reader.string(text)
            }
        }

        @throws[ModelException]
        private def readFiles(file: File): Seq[(File,Module)] = {
            wrapExceptions(file.toString) {
                if (file.isDirectory()) {
                    logger.info(s"Reading all module files in directory ${file.toString}")
                    val patterns = reader.globPatterns.map(GlobPattern(_))
                    file.list()
                        .par
                        .filter(f => f.isFile() && patterns.exists(_.matches(f.name)))
                        .map(f => f -> loadFile(f))
                        .seq
                }
                else {
                    Seq(file -> loadFile(file))
                }
            }
        }

        private def loadFile(file:File) : Module = {
            logger.info(s"Reading module from ${file.toString}")
            reader.file(file)
        }

        private lazy val reader : ModuleReader = {
            loader.find(_.supports(format))
                .getOrElse(throw new UnsupportedModuleFormatException(format))
        }

        private def wrapExceptions[T](source: String)(fn: => T): T = {
            try {
                fn
            }
            catch {
                case ex: ModelException => throw ex
                case NonFatal(ex) => throw new ModuleLoadException(source, ex)
            }
        }
    }

    def read = new Reader
}


final case class Module(
    config : Map[String,String] = Map.empty,
    environment : Map[String,String] = Map.empty,

    profiles : Map[String,Profile] = Map.empty,
    relations : Map[String,Prototype[Relation]] = Map.empty,
    connections : Map[String,Prototype[Connection]] = Map.empty,
    mappings : Map[String,Prototype[Mapping]] = Map.empty,
    targets : Map[String,Prototype[Target]] = Map.empty,
    jobs : Map[String,Prototype[Job]] = Map.empty,
    tests : Map[String,Prototype[Test]] = Map.empty,
    templates: Map[String,Prototype[Template[_]]] = Map.empty
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
            tests = tests ++ other.tests,
            templates = templates ++ other.templates
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
            tests = tests,
            templates = templates
        )
    }
}



