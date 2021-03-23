/*
 * Copyright 2019 Kaya Kupferschmidt
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
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.NoSuchMappingException
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.spec.splitSettings
import com.dimajix.flowman.spec.target.FileTarget
import com.dimajix.flowman.tools.exec.ActionCommand


class SaveCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[SaveCommand])

    @Option(name="-f", aliases=Array("--format"), usage="Specifies the format", metaVar="<format>", required = false)
    var format: String = "csv"
    @Option(name="-o", aliases=Array("--option"), usage = "additional format specific option", metaVar = "<key=value>", required = false)
    var options: Array[String] = Array()
    @Argument(usage = "specifies the mapping to save", metaVar = "<mapping>", required = true, index = 0)
    var mapping: String = ""
    @Argument(usage = "specifies the output filename", metaVar = "<filename>", required = true, index = 1)
    var location: String = ""

    override def executeInternal(session: Session, context:Context, project: Project) : Boolean = {
        val task = FileTarget(context, MappingOutputIdentifier(mapping), new Path(location), format, splitSettings(options).toMap)

        try {
            task.execute(session.execution, Phase.BUILD)
            true
        }
        catch {
            case ex:NoSuchMappingException =>
                logger.error(s"Cannot resolve mapping '${ex.mapping}'")
                false
            case NonFatal(e) =>
                logger.error(s"Caught exception while save mapping '$mapping'", e)
                false
        }
    }
}
