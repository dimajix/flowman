/*
 * Copyright (C) 2018 The Flowman Authors
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

import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.model.Result
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetResult


class SimpleExecutor(execution: Execution, context:Context) extends Executor {
    private val logger = execution.loggerFactory.getLogger(classOf[SimpleExecutor].getName)

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
    def execute(phase: Phase, targets: Seq[Target], filter:Target => Boolean, keepGoing: Boolean)(fn:(Execution,Target,Phase) => TargetResult) : Seq[TargetResult] = {
        val clazz = execution.flowmanConf.getConf(FlowmanConf.EXECUTION_SCHEDULER_CLASS)
        val scheduler = Scheduler.newInstance(clazz, execution, context)

        // First determine ordering before filtering active targets, since their might be some transitive dependencies
        // in place. For example accessing a VIEW which does not require a BUILD but accesses other resources
        val orderedTargets = Scheduler.sort(scheduler, targets, phase, filter)

        logger.info(s"Target order for $phase:")
        orderedTargets.foreach(t => logger.info("  - " + t.identifier))
        logger.info("")

        Result.map(orderedTargets, keepGoing) { target =>
            fn(execution, target, phase)
        }
    }
}
