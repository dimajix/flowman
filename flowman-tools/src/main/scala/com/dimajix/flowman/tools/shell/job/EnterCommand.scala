/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.tools.shell.job

import scala.util.control.NonFatal

import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.NoSuchJobException
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.common.ParserUtils.splitSettings
import com.dimajix.flowman.tools.exec.Command
import com.dimajix.flowman.tools.shell.Shell


class EnterCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[EnterCommand])

    @Argument(index=0, required=true, usage = "name of job to enter", metaVar = "<job>")
    var job: String = ""
    @Argument(index=1, required=false, usage = "specifies job parameters", metaVar = "<param>=<value>")
    var args: Array[String] = Array()

    override def execute(session: Session, project:Project, context:Context): Boolean = {
        try {
            val job = context.getJob(JobIdentifier(this.job))
            val args = splitSettings(this.args).toMap
            Shell.instance.enterJob(job, args)
            true
        }
        catch {
            case ex:NoSuchJobException =>
                logger.error(s"Cannot resolve job '${ex.job}'")
                false
            case NonFatal(e) =>
                logger.error(s"Error entering job '$job': ${e.getMessage}")
                false
        }
    }
}
