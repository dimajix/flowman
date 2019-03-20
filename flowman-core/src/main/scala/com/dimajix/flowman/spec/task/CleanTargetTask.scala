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
import com.dimajix.flowman.spec.TargetIdentifier


object CleanTargetTask {
    def apply(outputs:Seq[String], description:String) : CleanTargetTask = {
        val task = new CleanTargetTask
        task._targets = outputs
        task._description = description
        task
    }
}


class CleanTargetTask extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[BuildTargetTask])

    @JsonProperty(value="targets", required=true) private var _targets:Seq[String] = Seq()

    def targets(implicit context: Context) : Seq[TargetIdentifier] = _targets.map(i => TargetIdentifier.parse(context.evaluate(i)))

    /**
      * Executes all outputs defined in this task
      *
      * @param executor
      * @return
      */
    override def execute(executor:Executor) : Boolean = {
        implicit val context = executor.context
        targets.foreach(o => executeTarget(executor, o))
        true
    }

    private def executeTarget(executor: Executor, identifier:TargetIdentifier) : Boolean = {
        implicit val context = executor.context
        val target = context.getTarget(identifier)

        logger.info("Cleaning target '{}'", identifier.toString)
        target.clean(executor)
        true
    }
}
