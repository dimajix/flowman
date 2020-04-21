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

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.slf4j.LoggerFactory

import com.dimajix.flowman.hadoop.File


object Project {
    private lazy val loader = ServiceLoader.load(classOf[ProjectReader]).iterator().asScala.toSeq

    class Reader {
        private val logger = LoggerFactory.getLogger(classOf[Reader])
        private var format = "yaml"

        def format(fmt:String) : Reader = {
            format = fmt
            this
        }

        /**
         * Loads a project file and all related module files
         *
         * @param file
         * @return
         */
        def file(file: File): Project = {
            if (!file.isAbsolute()) {
                readFile(file.absolute)
            }
            else {
                readFile(file)
            }
        }

        /**
         * Read in only main definition file without additonal modules
         *
         * @param file
         * @return
         */
        def manifest(file: File): Project = {
            if (!file.isAbsolute()) {
                manifest(file.absolute)
            }
            else if (file.isDirectory) {
                logger.info(s"Reading project manifest in directory $file")
                manifest(file / "project.yml")
            }
            else {
                logger.info(s"Reading project manifest from $file")
                val project = reader.file(file)
                project.copy(
                    filename = Some(file.absolute),
                    basedir = Some(file.absolute.parent)
                )
            }
        }

        def string(text: String): Project = {
            reader.string(text)
        }

        private def readFile(file: File): Project = {
            if (file.isDirectory) {
                logger.info(s"Reading project in directory $file")
                this.file(file / "project.yml")
            }
            else {
                logger.info(s"Reading project from $file")
                val spec = reader.file(file)
                val project = loadModules(spec, file.parent)
                project.copy(
                    filename = Some(file.absolute),
                    basedir = Some(file.absolute.parent)
                )
            }
        }

        private def loadModules(project: Project, directory: File): Project = {
            val module = project.modules
                .map(f => Module.read.file(directory / f))
                .reduce((l, r) => l.merge(r))

            project.copy(
                environment = module.environment,
                config = module.config,
                profiles = module.profiles,
                connections = module.connections,
                relations = module.relations,
                mappings = module.mappings,
                targets = module.targets,
                jobs = module.jobs
            )
        }

        private def reader : ProjectReader = {
            loader.find(_.supports(format))
                .getOrElse(throw new IllegalArgumentException(s"Project format '$format' not supported'"))
        }
    }

    def read = new Reader()
}


final case class Project(
    name : String,
    description : Option[String] = None,
    version : Option[String] = None,
    filename : Option[File] = None,
    basedir : Option[File] = None,

    modules : Seq[String] = Seq(),

    config : Map[String,String] = Map(),
    environment : Map[String,String] = Map(),

    profiles : Map[String,Profile] = Map(),
    relations : Map[String,Template[Relation]] = Map(),
    connections : Map[String,Template[Connection]] = Map(),
    mappings : Map[String,Template[Mapping]] = Map(),
    targets : Map[String,Template[Target]] = Map(),
    jobs : Map[String,Template[Job]] = Map()
)


abstract class ProjectReader {
    def name : String
    def format : String

    def supports(format:String) : Boolean = this.format == format

    def file(file: File) : Project
    def string(text: String): Project
}
