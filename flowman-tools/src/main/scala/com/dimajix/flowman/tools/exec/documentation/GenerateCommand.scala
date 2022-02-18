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

package com.dimajix.flowman.tools.exec.documentation

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.common.ParserUtils.splitSettings
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.spec.documentation.DocumenterLoader
import com.dimajix.flowman.tools.exec.Command


class GenerateCommand extends Command {
    private val logger = LoggerFactory.getLogger(getClass)

    @Argument(index=0, required=false, usage = "specifies job to document", metaVar = "<job>")
    var job: String = "main"
    @Argument(index=1, required=false, usage = "specifies job parameters", metaVar = "<param>=<value>")
    var args: Array[String] = Array()

    override def execute(session: Session, project: Project, context:Context) : Status = {
        val args = splitSettings(this.args).toMap
        Try {
            context.getJob(JobIdentifier(job))
        }
        match {
            case Failure(e) =>
                logger.error(s"Error instantiating job '$job': ${reasons(e)}")
                Status.FAILED
            case Success(job) =>
                generateDoc(session, project, job, job.arguments(args))
        }
    }

    private def generateDoc(session: Session, project:Project, job:Job, args:Map[String,Any]) : Status = {
        val documenter = DocumenterLoader.load(job.context, project)
        try {
            documenter.execute(session, job, args)
            Status.SUCCESS
        } catch {
            case NonFatal(ex) =>
                logger.error("Cannot generate documentation: ", ex)
                Status.FAILED
        }
    }
}
