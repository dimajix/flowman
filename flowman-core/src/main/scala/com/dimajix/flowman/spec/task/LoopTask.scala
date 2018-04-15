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


class LoopTask extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[LoopTask])

    @JsonProperty(value="items", required=true) private var _items:String = _
    @JsonProperty(value="var", required=true) private var _var:String = "item"
    @JsonProperty(value="tasks") private var _tasks:Seq[Task] = Seq()
    @JsonProperty(value="job") private var _job:String = _

    def tasks : Seq[Task] = _tasks
    def job(implicit context:Context) : String = context.evaluate(_job)

    override def execute(executor:Executor) : Boolean = {
        implicit val context = executor.context
        val impl = if (Option(job).exists(_.nonEmpty))
            executeJob _
        else
            executeTasks _

        impl(executor)
    }

    private def executeJob(executor: Executor) : Boolean = {
        implicit val context = executor.context
        logger.info(s"Running job: '$job'")
        false
    }
    private def executeTasks(executor: Executor) : Boolean = ???
}
