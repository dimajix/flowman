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

package com.dimajix.flowman.spec.task

import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.JobIdentifier
import com.dimajix.flowman.state.Status


object CallTask {
    def apply(job:Job, args:Map[String,String], force:Boolean=false) : CallTask = {
        CallTask(
            Task.Properties(null),
            job,
            args,
            force
        )
    }

}

case class CallTask(
    instanceProperties:Task.Properties,
    job:Job,
    args:Map[String,String],
    force:Boolean
) extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[CallTask])

    override def execute(executor:Executor) : Boolean = {
        executeJob(executor)
    }

    private def executeJob(executor: Executor) : Boolean = {
        logger.info(s"Calling sub-job '${job.identifier}' with args ${args.map(kv => kv._1 + "=" + kv._2).mkString(", ")}")

        executor.runner.execute(executor, job, args, force) match {
            case Status.SUCCESS => true
            case Status.SKIPPED => true
            case _ => false
        }
    }
}



class CallTaskSpec extends TaskSpec {
    @JsonProperty(value = "job", required = true) private var job: String = ""
    @JsonProperty(value = "force") private var force: String = "false"
    @JsonProperty(value = "args", required = false) private var args: Map[String, String] = Map()


    override def instantiate(context: Context): CallTask = {
        CallTask(
            instanceProperties(context),
            context.getJob(JobIdentifier.parse(context.evaluate(job))),
            args.mapValues(context.evaluate),
            context.evaluate(force).toBoolean
        )
    }
}
