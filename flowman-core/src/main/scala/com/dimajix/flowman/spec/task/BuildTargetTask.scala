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
import com.dimajix.flowman.spec.TargetIdentifier
import com.dimajix.flowman.spec.target.Target
import com.dimajix.flowman.state.Status


object BuildTargetTask {
    def apply(targets:Seq[Target], description:String) : BuildTargetTask = {
        BuildTargetTask(
            Task.Properties(null),
            targets
        )
    }
}

case class BuildTargetTask(instanceProperties:Task.Properties, targets:Seq[Target]) extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[BuildTargetTask])

    /**
      * Executes all outputs defined in this task
      *
      * @param executor
      * @return
      */
    override def execute(executor:Executor) : Boolean = {
        targets.forall(o => executeTarget(executor, o))
    }

    private def executeTarget(executor: Executor, target:Target) : Boolean = {
        val result = executor.runner.build(executor, target)

        // Only return true if status is SUCCESS or SKIPPED
        result match {
            case Status.SUCCESS | Status.SKIPPED => true
            case _ => false
        }
    }
}



class BuildTargetTaskSpec extends TaskSpec {
    @JsonProperty(value="targets", required=true) private var _targets:Seq[String] = Seq()

    override def instantiate(context: Context): BuildTargetTask = {
        BuildTargetTask(
            instanceProperties(context),
            _targets.map(i => context.getTarget(TargetIdentifier(context.evaluate(i))))
        )
    }
}
