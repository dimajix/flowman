/*
 * Copyright 2018 Kaya Kupferschmidt
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


class CallTask extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[CallTask])

    @JsonProperty(value="job", required=true) private var _job:String = ""
    @JsonProperty(value="force") private var _force:String = "false"
    @JsonProperty(value="args", required=false) private var _args:Map[String,String] = Map()

    def this(job:String, args:Map[String,String], force:String="false") = {
        this()
        _job = job
        _args = args
        _force = force
    }

    def job(implicit context:Context) : JobIdentifier = JobIdentifier.parse(context.evaluate(_job))
    def args(implicit context:Context) : Map[String,String] = _args.mapValues(context.evaluate)
    def force(implicit context: Context) : Boolean = context.evaluate(_force).toBoolean

    override def execute(executor:Executor) : Boolean = {
        executeJob(executor)
    }

    private def executeJob(executor: Executor) : Boolean = {
        implicit val context = executor.context
        logger.info(s"Calling sub-job '${this.job}' with args ${args.map(kv => kv._1 + "=" + kv._2).mkString(", ")}")

        val job = context.getJob(this.job)
        context.runner.execute(executor, job, args, force) match {
            case JobStatus.SUCCESS => true
            case JobStatus.SKIPPED => true
            case _ => false
        }
    }
}
