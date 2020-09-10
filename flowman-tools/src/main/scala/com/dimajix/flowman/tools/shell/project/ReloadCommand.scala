/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.tools.shell.project

import scala.util.control.NonFatal

import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.tools.exec.Command
import com.dimajix.flowman.tools.shell.Shell


class ReloadCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[ReloadCommand])

    override def execute(session: Session, project:Project, context:Context): Boolean = {
        project.filename.map { fn =>
            try {
                Shell.instance.loadProject(fn.path)
                true
            }
            catch {
                case NonFatal(e) =>
                    logger.error(s"Error reloading current project '${fn}': ${e.getMessage}")
                    false
            }
        }.getOrElse {
            logger.warn(s"Cannot reload current project, since it has no path")
            false
        }
    }
}
