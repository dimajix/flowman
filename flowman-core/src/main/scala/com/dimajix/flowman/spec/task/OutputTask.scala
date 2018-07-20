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
import com.dimajix.flowman.spec.OutputIdentifier


object OutputTask {
    def apply(outputs:Seq[String], description:String) = {
        val task = new OutputTask
        task._outputs = outputs
        task._description = description
        task
    }
}


class OutputTask extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[OutputTask])

    @JsonProperty(value="outputs", required=true) private var _outputs:Seq[String] = Seq()

    def outputs(implicit context: Context) : Seq[OutputIdentifier] = _outputs.map(i => OutputIdentifier.parse(context.evaluate(i)))

    /**
      * Executes all outputs defined in this task
      *
      * @param executor
      * @return
      */
    override def execute(executor:Executor) : Boolean = {
        implicit val context = executor.context
        outputs.foreach(o => executeOutput(executor, o))
        true
    }

    private def executeOutput(executor: Executor, identifier:OutputIdentifier) : Boolean = {
        implicit val context = executor.context
        val output = context.getOutput(identifier)

        logger.info("Resolving dependencies for output '{}'", identifier.toString)
        val dependencies = output.dependencies
            .map(d => (d, executor.instantiate(d)))
            .toMap

        logger.info("Executing output '{}'", identifier.toString)
        output.execute(executor, dependencies)
        true
    }
}
