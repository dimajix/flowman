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

package com.dimajix.flowman.execution

import org.slf4j.LoggerFactory

import com.dimajix.flowman.state.JobInstance


class SimpleRunner extends AbstractRunner {
    override protected val logger = LoggerFactory.getLogger(classOf[SimpleRunner])

    /**
      * Performs some checkJob, if the run is required
      * @param context
      * @return
      */
    override protected def check(context:Context, job:JobInstance) : Boolean = false

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @param context
      * @return
      */
    override protected def start(context:Context, job:JobInstance) : Object = null

    /**
      * Marks a run as a success
      *
      * @param context
      * @param token
      */
    override protected def success(context: Context, token:Object) : Unit = {}

    /**
      * Marks a run as a failure
      *
      * @param context
      * @param token
      */
    override protected def failure(context: Context, token:Object) : Unit = {}

    /**
      * Marks a run as a failure
      *
      * @param context
      * @param token
      */
    override protected def aborted(context: Context, token:Object) : Unit = {}

    /**
      * Marks a run as being skipped
      *
      * @param context
      * @param token
      */
    override protected def skipped(context: Context, token:Object) : Unit = {}
}
