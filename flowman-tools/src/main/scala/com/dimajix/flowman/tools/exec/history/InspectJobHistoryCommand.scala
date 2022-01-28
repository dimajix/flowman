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

package com.dimajix.flowman.tools.exec.history

import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.history.JobQuery
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.tools.exec.Command


class InspectJobHistoryCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[InspectJobHistoryCommand])

    @Argument(usage = "Job run ID", metaVar = "<job_run_id>", required = true)
    var jobId: String = ""

    override def execute(session: Session, project: Project, context: Context): Status = {
        val query = JobQuery(
            id = Seq(jobId)
        )
        session.history.findJobs(query).headOption match {
            case Some(job) =>
                println(s"Job run id: ${job.id}")
                println(s"  Namespace: ${job.namespace}")
                println(s"  Project: ${job.project}")
                println(s"  Project version: ${job.version}")
                println(s"  Job name: ${job.job}")
                println(s"  Phase: ${job.phase}")
                println(s"  Status: ${job.status}")
                println(s"  Start date: ${job.startDateTime.map(_.toString).getOrElse("")}")
                println(s"  End date: ${job.endDateTime.map(_.toString).getOrElse("")}")
                println(s"Job arguments:")
                job.args.foreach { m =>
                    println(s"  ${m._1} = ${m._2}")
                }
                val metrics = session.history.getJobMetrics(job.id)
                println("Job execution metrics:")
                metrics.foreach { m =>
                    println(s"  ${m.name} ts=${m.ts} labels=${m.labels.map(kv => kv._1 + "=" + kv._2).mkString("(",",",")")} value=${m.value}")
                }
                Status.SUCCESS
            case None =>
                logger.error(s"Cannot find job run with id '$jobId'")
                Status.FAILED
        }
    }
}
