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

package com.dimajix.flowman.execution

import org.slf4j.LoggerFactory

import com.dimajix.flowman.state.JobInstance
import com.dimajix.flowman.state.JobToken
import com.dimajix.flowman.state.TargetInstance
import com.dimajix.flowman.state.TargetToken


class SimpleRunner extends AbstractRunner {
    override protected val logger = LoggerFactory.getLogger(classOf[SimpleRunner])

    override protected def jobRunner(job:JobToken) : Runner = this

    /**
      * Performs some checkJob, if the run is required. Returns false if the job is out of date and should be rerun
      * @param context
      * @return
      */
    override protected def checkJob(context:Context, job:JobInstance) : Boolean = false

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @param context
      * @return
      */
    override protected def startJob(context:Context, job:JobInstance, parent:Option[JobToken]) : JobToken = null

    /**
      * Marks a run as a success
      *
      * @param context
      * @param token
      */
    override protected def finishJob(context: Context, token:JobToken) : Unit = {}

    /**
      * Marks a run as a failure
      *
      * @param context
      * @param token
      */
    override protected def failJob(context: Context, token:JobToken) : Unit = {}

    /**
      * Marks a run as a failure
      *
      * @param context
      * @param token
      */
    override protected def abortJob(context: Context, token:JobToken) : Unit = {}

    /**
      * Marks a run as being skipped
      *
      * @param context
      * @param token
      */
    override protected def skipJob(context: Context, token:JobToken) : Unit = {}

    /**
      * Performs some checks, if the run is required. Returns faöse if the target is out of date needs to be rebuilt
      *
      * @param target
      * @return
      */
    protected override def checkTarget(context: Context, target: TargetInstance): Boolean = false

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @param target
      * @return
      */
    override protected def startTarget(context: Context, target:TargetInstance, parent:Option[JobToken]) : TargetToken = null

    /**
      * Marks a run as a success
      *
      * @param token
      */
    override protected def finishTarget(context: Context, token:TargetToken) : Unit = {}

    /**
      * Marks a run as a failure
      *
      * @param token
      */
    override protected def failTarget(context: Context, token:TargetToken) : Unit = {}

    /**
      * Marks a run as being skipped
      *
      * @param token
      */
    override protected def skipTarget(context: Context, token:TargetToken) : Unit = {}
}
