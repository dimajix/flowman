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

package com.dimajix.flowman.tools.shell.test

import scala.util.control.NonFatal

import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.NoSuchTestException
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.TestIdentifier
import com.dimajix.flowman.tools.exec.Command
import com.dimajix.flowman.tools.shell.Shell


class EnterCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[EnterCommand])

    @Argument(index=0, required=true, usage = "name of test to enter", metaVar = "<test>")
    var test: String = ""

    override def execute(session: Session, project:Project, context:Context): Status = {
        try {
            val test = context.getTest(TestIdentifier(this.test))
            Shell.instance.enterTest(test)
            Status.SUCCESS
        }
        catch {
            case ex:NoSuchTestException =>
                logger.error(s"Cannot resolve test '${ex.test}'")
                Status.FAILED
            case NonFatal(e) =>
                logger.error(s"Error entering test '$test':\n  ${reasons(e)}")
                Status.FAILED
        }
    }
}
