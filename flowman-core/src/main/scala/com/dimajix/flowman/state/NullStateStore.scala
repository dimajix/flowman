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

package com.dimajix.flowman.state

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.task.Job


class NullStateStore extends StateStore {
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
    override def startJob(job:JobInstance) : Object = null

    /**
      * Marks a run as a success
      *
      * @param token
      */
    override def success(token:Object) : Unit = {}

    /**
      * Marks a run as a failure
      *
      * @param token
      */
    override def failure(token:Object) : Unit = {}

    /**
      * Marks a run as a failure
      *
      * @param token
      */
    override def aborted(token:Object) : Unit = {}

    /**
      * Marks a run as being skipped
      *
      * @param token
      */
    override def skipped(token:Object) : Unit = {}
}
