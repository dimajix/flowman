/*
 * Copyright (C) 2019 The Flowman Authors
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

package com.dimajix.flowman.tools.exec.mapping

import scala.util.Failure
import scala.util.Success

import org.apache.hadoop.fs.Path
import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.common.ParserUtils.splitSettings
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.NoSuchMappingException
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.spec.target.FileTarget
import com.dimajix.flowman.tools.exec.Command


class SaveCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[SaveCommand])

    @Option(name="-f", aliases=Array("--format"), usage="Specifies the format", metaVar="<format>", required = false)
    var format: String = "csv"
    @Option(name="-o", aliases=Array("--option"), usage = "additional format specific option", metaVar = "<key=value>", required = false)
    var options: Array[String] = Array()
    @Argument(usage = "specifies the mapping to save", metaVar = "<mapping>", required = true, index = 0)
    var mapping: String = ""
    @Argument(usage = "specifies the output filename", metaVar = "<filename>", required = true, index = 1)
    var location: String = ""

    override def execute(session: Session, project: Project, context:Context) : Status = {
        val task = FileTarget(context, MappingOutputIdentifier(mapping), new Path(location), format, splitSettings(options).toMap)

        task.execute(session.execution, Phase.BUILD).toTry match {
            case Success(s) =>
                s
            case Failure(ex:NoSuchMappingException) =>
                logger.error(s"Cannot resolve mapping '${ex.mapping}'")
                Status.FAILED
            case Failure(e) =>
                logger.error(s"Caught exception while save mapping '$mapping':\n  ${reasons(e)}")
                Status.FAILED
        }
    }
}
