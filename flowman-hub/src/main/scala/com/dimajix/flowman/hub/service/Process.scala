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

package com.dimajix.flowman.hub.service

import org.reactivestreams.Publisher


sealed abstract class ProcessState
object ProcessState {
    case object STARTING extends ProcessState
    case object RUNNING extends ProcessState
    case object STOPPING extends ProcessState
    case object TERMINATED extends ProcessState
}


abstract class Process {
    /**
     * Tries to shutdown the process
     */
    def shutdown() : Unit

    /**
     * Returns the current state of the process
     * @return
     */
    def state : ProcessState

    /**
     * Returns a publisher for all console messages produced by the process
     * @return
     */
    def messages : Publisher[String]
}
