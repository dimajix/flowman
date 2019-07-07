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

package com.dimajix.flowman.history


class NullStateStore extends StateStore {
    /**
      * Returns the state of a job
      * @param job
      * @return
      */
    def getJobState(job:JobInstance) : Option[JobState] = None

    /**
      * Performs some checkJob, if the run is required
      * @param job
      * @return
      */
    override def checkJob(job:JobInstance) : Boolean = false

    /**
      * Starts the run and returns a token, which can be anything
      * @param job
      * @return
      */
    override def startJob(job:JobInstance, parent:Option[JobToken]) : JobToken = null

    /**
      * Sets the status of a job after it has been started
      * @param token
      * @param status
      */
    override def finishJob(token:JobToken, status:Status) : Unit = {}

    /**
      * Returns the state of a target
      * @param target
      * @return
      */
    def getTargetState(target:TargetInstance) : Option[TargetState] = None

    /**
      * Performs some checkTarget, if the run is required
      * @param target
      * @return
      */
    override def checkTarget(target:TargetInstance) : Boolean = false

    /**
      * Starts the run and returns a token, which can be anything
      * @param target
      * @return
      */
    override def startTarget(target:TargetInstance, parent:Option[JobToken]) : TargetToken = null

    /**
      * Sets the status of a target after it has been started
      * @param token
      * @param status
      */
    override def finishTarget(token:TargetToken, status:Status) : Unit = {}
}
