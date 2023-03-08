/*
 * Copyright 2021 Kaya Kupferschmidt
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

import com.dimajix.flowman.model.Target


class ManualScheduler(execution: Execution, context: Context) extends Scheduler {
    private var targets = Seq[Target]()

    /**
     * Initializes the [[Scheduler]] with a new list of targets to be processed. Note that the specified
     * list of targets may be larger than what actually should be processed. The [[Scheduler]] needs to
     * take into account the processing [[Phase]] and the given filter
     *
     * @param targets Contains the list of all targets required for finding a correct sequence
     * @param phase   Specified the processing phase
     * @param filter  Specifies a filter
     */
    override def initialize(targets: Seq[Target], phase: Phase, filter: Target => Boolean): Unit = {
        this.targets =
            phase match {
                case Phase.DESTROY | Phase.TRUNCATE => targets.filter(t => t.phases.contains(phase) && filter(t)).reverse
                case _ => targets.filter(t => t.phases.contains(phase) && filter(t))
            }
    }

    /**
     * Returns the next target to be processed. The method may return [[None]] in case that there are still
     * unprocessed targets, but their dependencies are still not processed
     *
     * @return
     */
    override def next(): Option[Target] = {
        if (targets.nonEmpty) {
            val next = targets.head
            targets = targets.tail
            Some(next)
        }
        else {
            None
        }
    }

    /**
     * Returns [[true]] if more work is to be done. But [[true]] does not mandate that [[next]] will return a
     * valid target, instead it may also return [[None]]
     *
     * @return
     */
    override def hasNext(): Boolean = targets.nonEmpty

    /**
     * Marks a given target as complete after it has been processed. This method will unblock all targets waiting
     * for this target to be finished
     *
     * @param target
     */
    override def complete(target: Target): Unit = {}
}
