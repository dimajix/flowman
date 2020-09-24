/*
 * Copyright 2018-2020 Kaya Kupferschmidt
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

import com.dimajix.flowman.model.JobInstance
import com.dimajix.flowman.model.TargetInstance


abstract class JobToken
abstract class TargetToken


trait JobListener {
    /**
     * Starts the run and returns a token, which can be anything
     * @param job
     * @return
     */
    def startJob(job:JobInstance, phase:Phase) : JobToken

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param status
     */
    def finishJob(token:JobToken, status:Status) : Unit

    /**
     * Starts the run and returns a token, which can be anything
     * @param target
     * @return
     */
    def startTarget(target:TargetInstance, phase:Phase, parent:Option[JobToken]) : TargetToken

    /**
     * Sets the status of a job after it has been started
     * @param token The token returned by startJob
     * @param status
     */
    def finishTarget(token:TargetToken, status:Status) : Unit

}
