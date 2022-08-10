/*
 * Copyright 2020-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.tools.shell

import scala.util.control.NonFatal

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.spi.RestOfArgumentsHandler
import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.tools.exec.Command


class EvaluateCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[EvaluateCommand])

    @Argument(index=0, required=true, usage = "expression to evaluate", metaVar = "<expr>", handler=classOf[RestOfArgumentsHandler])
    var args: Array[String] = Array()

    override def execute(session: Session, project:Project, context:Context): Status = {
        try {
            println(context.evaluate(args.mkString(" ")))
            Status.SUCCESS
        }
        catch {
            case NonFatal(e) =>
                logger.error(s"Error evaluating expression: ${reasons(e)}")
                Status.FAILED
        }
    }
}
