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

import java.util.ServiceLoader

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.slf4j.LoggerFactory

import com.dimajix.flowman.fs.File
import com.dimajix.flowman.spi.ProjectReader



object Project {
    private lazy val loader = ServiceLoader.load(classOf[ProjectReader]).iterator().asScala.toSeq

    case class Import(
        project:String,
        job:Option[String] = None,
        arguments:Map[String,String] = Map.empty
    )

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
        @throws[ModelException]
        def file(file: File): Project = {
            if (!file.isAbsolute()) {
                this.file(file.absolute)
            }
            else {
                logger.info(s"Reading project from $file")
                val spec = wrapExceptions(file.toString) {
                    reader.file(file)
                }
                loadModules(spec, spec.basedir.getOrElse(file))
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
            else {
                logger.info(s"Reading project manifest from $file")
                wrapExceptions(file.toString) {
                    reader.file(file)
                }
            }
        }

        @throws[ModelException]
        def string(text: String): Project = {
            wrapExceptions("raw-string") {
                reader.string(text)
            }
        }

        @throws[ModelException]
        private def loadModules(project: Project, directory: File): Project = {
            val modules = project.modules
                .par
                .flatMap(f => Module.read.files(directory / f))

            // Check for duplicate entities in different modules
            def checkDuplicates(category: Category, fn: Module => Map[String, _]): Unit = {
                modules.map { case (file,module) => fn(module).map { case(name,_) => name -> file } }
                    .reduceLeftOption { (all, next) =>
                        next.keySet.find(all.contains).foreach { name =>
                            val prevFile = all(name)
                            val newFile = next(name)
                            throw new DuplicateEntityException(name, category, prevFile.toString, newFile.toString)
                        }
                        all ++ next
                    }
            }
            checkDuplicates(Category.CONNECTION, _.connections)
            checkDuplicates(Category.RELATION, _.relations)
            checkDuplicates(Category.MAPPING, _.mappings)
            checkDuplicates(Category.TARGET, _.targets)
            checkDuplicates(Category.JOB, _.jobs)
            checkDuplicates(Category.TEST, _.tests)
            checkDuplicates(Category.TEMPLATE, _.templates)
            checkDuplicates(Category.CONFIG, _.config)
            checkDuplicates(Category.ENVIRONMENT, _.environment)
            checkDuplicates(Category.PROFILE, _.profiles)

            // Now merge all modules into a single one
            val module = modules.foldLeft(Module())((l, r) => l.merge(r._2))

            logger.info(s"Loaded project '${project.name}'${project.version.map(v => s" version $v").getOrElse("")}")

            project.copy(
                environment = module.environment,
                config = module.config,
                profiles = module.profiles,
                connections = module.connections,
                relations = module.relations,
                mappings = module.mappings,
                targets = module.targets,
                jobs = module.jobs,
                tests = module.tests,
                templates = module.templates
            )
        }

        private def reader : ProjectReader = {
            loader.find(_.supports(format))
                .getOrElse(throw new UnsupportedProjectFormatException(format))
        }

        private def wrapExceptions[T](source:String)(fn: => T) : T = {
            try {
                fn
            }
            catch {
                case ex: ModelException => throw ex
                case NonFatal(ex) => throw new ProjectLoadException(source, ex)
            }
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

    modules : Seq[String] = Seq.empty,

    config : Map[String,String] = Map.empty,
    environment : Map[String,String] = Map.empty,

    imports: Seq[Project.Import] = Seq.empty,
    profiles : Map[String,Profile] = Map.empty,

    relations : Map[String,Prototype[Relation]] = Map.empty,
    connections : Map[String,Prototype[Connection]] = Map.empty,
    mappings : Map[String,Prototype[Mapping]] = Map.empty,
    targets : Map[String,Prototype[Target]] = Map.empty,
    jobs : Map[String,Prototype[Job]] = Map.empty,
    tests : Map[String,Prototype[Test]] = Map.empty,
    templates: Map[String,Prototype[Template[_]]] = Map.empty
)
