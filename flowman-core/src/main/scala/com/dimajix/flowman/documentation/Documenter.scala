/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.documentation

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.graph.Graph
import com.dimajix.flowman.hadoop.File
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.spi.DocumenterReader


object Documenter {
    private lazy val loader = ServiceLoader.load(classOf[DocumenterReader]).iterator().asScala.toSeq
    private lazy val defaultDocumenter = {
        val collectors = Seq(
            new RelationCollector(),
            new MappingCollector(),
            new TargetCollector(),
            new LineageCollector(),
            new CheckCollector()
        )
        Documenter(
            collectors=collectors
        )
    }

    class Reader {
        private val logger = LoggerFactory.getLogger(classOf[Documenter])
        private var format = "yaml"

        def default() : Documenter = defaultDocumenter

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
        def file(file:File) : Prototype[Documenter] = {
            if (!file.isAbsolute()) {
                this.file(file.absolute)
            }
            else {
                logger.info(s"Reading documenter from ${file.toString}")
                reader.file(file)
            }
        }

        def string(text:String) : Prototype[Documenter] = {
            reader.string(text)
        }

        private def reader : DocumenterReader = {
            loader.find(_.supports(format))
                .getOrElse(throw new IllegalArgumentException(s"Module format '$format' not supported'"))
        }
    }

    def read = new Reader
}


final case class Documenter(
    collectors:Seq[Collector] = Seq(),
    generators:Seq[Generator] = Seq()
) {
    def execute(session:Session, job:Job, args:Map[String,Any]) : Unit = {
        val runner = session.runner
        runner.withExecution(isolated=true) { execution =>
            runner.withJobContext(job, args, Some(execution)) { (context, arguments) =>
                execute(context, execution, job.project.get)
            }
        }
    }
    def execute(context:Context, execution: Execution, project:Project) : Unit = {
        // 1. Get Project documentation
        val projectDoc = ProjectDoc(
            project.name,
            version = project.version,
            description = project.description
        )

        // 2. Apply all other collectors
        val graph = Graph.ofProject(context, project, Phase.BUILD)
        val finalDoc = collectors.foldLeft(projectDoc)((doc, collector) => collector.collect(execution, graph, doc))

        // 3. Generate documentation
        generators.foreach { gen =>
            gen.generate(context, execution, finalDoc)
        }
    }
}
