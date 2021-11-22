/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.tools.exec.history

import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.history.JobQuery
import com.dimajix.flowman.history.TargetQuery
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.tools.exec.Command


class InspectTargetHistoryCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[InspectTargetHistoryCommand])

    @Argument(usage = "Target run ID", metaVar = "<target_run_id>", required = true)
    var targetId: String = ""

    override def execute(session: Session, project: Project, context: Context): Boolean = {
        val query = TargetQuery(
            id = Seq(targetId)
        )
        session.history.findTargetStates(query).headOption match {
            case Some(target) =>
                println(s"Target run id: ${target.id}")
                println(s"  Job run id: ${target.jobId}")
                println(s"  Namespace: ${target.namespace}")
                println(s"  Project: ${target.project}")
                println(s"  Project version: ${target.version}")
                println(s"  Target name: ${target.target}")
                println(s"  Phase: ${target.phase}")
                println(s"  Status: ${target.status}")
                println(s"  Start date: ${target.startDateTime.map(_.toString).getOrElse("")}")
                println(s"  End date: ${target.endDateTime.map(_.toString).getOrElse("")}")
                println(s"Target partitions:")
                target.partitions.foreach { m =>
                    println(s"  ${m._1} = ${m._2}")
                }
                true
            case None =>
                logger.error(s"Cannot find target run with id '$targetId'")
                false
        }
    }
}
