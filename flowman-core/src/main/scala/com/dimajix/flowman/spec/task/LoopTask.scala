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
import com.dimajix.flowman.spec.schema.FieldValue


class LoopTask extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[LoopTask])

    @JsonProperty(value="job") private var _job:String = _
    @JsonProperty(value="args", required=true) private var _args:Map[String,FieldValue] = Map()

    def this(job:String, args:Map[String,FieldValue]) = {
        this()
        _job = job
        _args = args
    }

    def job(implicit context:Context) : JobIdentifier = if (Option(_job).exists(_.nonEmpty)) JobIdentifier.parse(context.evaluate(_job)) else null
    def args : Map[String,FieldValue] = _args

    override def execute(executor:Executor) : Boolean = {
        executeJob(executor)
    }

    private def executeJob(executor: Executor) : Boolean = {
        implicit val context = executor.context
        logger.info(s"Running job: '${this.job}'")

        def interpolate(fn:Map[String,String] => Boolean, param:JobParameter, values:FieldValue) : Map[String,String] => Boolean = {
            val vals = param.ftype.interpolate(values, param.granularity).map(_.toString)
            (args:Map[String,String]) => vals.forall(v => fn(args + (param.name -> v)))
        }

        val job = context.getJob(this.job)
        val run = (args:Map[String,String]) => {
            context.runner.execute(executor, job, args) match {
                case JobStatus.SUCCESS => true
                case JobStatus.SKIPPED => true
                case _ => false
            }
        }

        // Iterate by all parameters and create argument map
        val paramByName = job.parameters.map(p => (p.name, p)).toMap
        val result = args.toSeq.foldLeft(run)((a,p) => interpolate(a, paramByName(p._1), p._2))

        result(Map())
    }
}
