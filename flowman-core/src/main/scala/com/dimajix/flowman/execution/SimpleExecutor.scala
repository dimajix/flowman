/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.execution

import scala.collection.mutable

import org.slf4j.LoggerFactory

import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.model.Result
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetResult


class SimpleExecutor extends Executor {
    private val logger = LoggerFactory.getLogger(classOf[SimpleExecutor])

    /**
     * Executes a list of targets in an appropriate order.
     *
     * @param execution
     * @param context
     * @param phase - Phase to execute
     * @param targets - List of all targets, even those which should not be executed
     * @param filter - Filter predicate to find all targets to be execution
     * @param keepGoing - True if errors in one target should not stop other targets from being executed
     * @param fn - Function to call. Note that the function is expected not to throw a non-fatal exception.
     * @return
     */
    def execute(execution: Execution, context:Context, phase: Phase, targets: Seq[Target], filter:Target => Boolean, keepGoing: Boolean)(fn:(Execution,Target,Phase) => TargetResult) : Seq[TargetResult] = {
        val clazz = execution.flowmanConf.getConf(FlowmanConf.EXECUTION_SCHEDULER_CLASS)
        val ctor = clazz.getDeclaredConstructor()
        val scheduler = ctor.newInstance()

        // First determine ordering before filtering active targets, since their might be some transitive dependencies
        // in place. For example accessing a VIEW which does not require a BUILD but accesses other resources
        val orderedTargets = Scheduler.sort(scheduler, targets, phase, filter)

        logger.info(s"Target order for $phase:")
        orderedTargets.foreach(t => logger.info("  - " + t.identifier))

        Result.map(orderedTargets, keepGoing) { target =>
            fn(execution, target, phase)
        }
    }
}
