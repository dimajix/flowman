/*
 * Copyright 2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.tools.exec.job

import scala.util.control.NonFatal

import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.NoSuchJobException
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.tools.exec.Command


class InspectCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[InspectCommand])

    @Argument(index=0, required=true, usage = "name of job to inspect", metaVar = "<job>")
    var job: String = ""

    override def execute(session: Session, project:Project, context:Context): Boolean = {
        try {
            val job = context.getJob(JobIdentifier(this.job))
            println(s"Name: ${job.name}")
            println(s"Description: ${job.description}")
            println("Targets:")
            job.targets
                .foreach{ p => println(s"    $p") }
            println("Parameters:")
            job.parameters
                .sortBy(_.name)
                .foreach{ p => println(s"    ${p.name} : ${p.ftype}") }
            println("Environment:")
            job.environment
                .toSeq
                .sortBy(_._1)
                .foreach{ case(k,v) => println(s"    $k=$v") }
            true
        }
        catch {
            case ex:NoSuchJobException =>
                logger.error(s"Cannot resolve job '${ex.job}'")
                false
            case NonFatal(e) =>
                logger.error(s"Error '$job': ${e.getMessage}")
                false
        }
    }
}
