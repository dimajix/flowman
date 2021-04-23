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

import scala.collection.mutable

import com.dimajix.flowman.model.Target


object Scheduler {
    def sort(scheduler: Scheduler, targets: Seq[Target], phase:Phase, filter:Target => Boolean) : Seq[Target] = {
        scheduler.initialize(targets, phase, filter)

        val orderedTargets = mutable.ListBuffer[Target]()
        while(scheduler.hasNext()) {
            scheduler.next() match {
                case Some(target) =>
                    scheduler.complete(target)
                    orderedTargets.append(target)
                case None =>
            }
        }

        orderedTargets
    }
}

abstract class Scheduler {
    /**
     * Initializes the [[Scheduler]] with a new list of targets to be processed. Note that the specified
     * list of targets may be larger than what actually should be processed. The [[Scheduler]] needs to
     * take into account the processing [[Phase]] and the given filter
 *
     * @param targets Contains the list of all targets required for finding a correct sequence
     * @param phase Specified the processing phase
     * @param filter Specifies a filter
     */
    def initialize(targets: Seq[Target], phase:Phase, filter:Target => Boolean) : Unit

    /**
     * Returns the next target to be processed. The method may return [[None]] in case that there are still
     * unprocessed targets, but their dependencies are still not processed
     * @return
     */
    def next() : Option[Target]

    /**
     * Returns [[true]] if more work is to be done. But [[true]] does not mandate that [[next]] will return a
     * valid target, instead it may also return [[None]]
     * @return
     */
    def hasNext() : Boolean

    /**
     * Marks a given target as complete after it has been processed. This method will unblock all targets waiting
     * for this target to be finished
     * @param target
     */
    def complete(target:Target) : Unit
}
