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

import com.dimajix.flowman.state.StateStore
import com.dimajix.flowman.state.JobInstance
import com.dimajix.flowman.state.Status


/**
  * This implementation of the Runner interface provides monitoring via calling appropriate methods in
  * a StateStoreProvider
  * @param monitor
  */
class MonitoredRunner(monitor:StateStore) extends AbstractRunner {
    override protected val logger = LoggerFactory.getLogger(classOf[MonitoredRunner])

    /**
      * Performs some checkJob, if the run is required
      * @param job
      * @return
      */
    protected override def check(context: Context, job:JobInstance) : Boolean = {
        monitor.checkJob(job)
    }

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @param job
      * @return
      */
    protected override def start(context: Context, job:JobInstance) : Object = {
        monitor.startJob(job)
    }

    /**
      * Marks a run as a success
      *
      * @param token
      */
    protected override def success(context: Context, token:Object) : Unit = {
        monitor.finishJob(token, Status.SUCCESS)
    }

    /**
      * Marks a run as a failure
      *
      * @param token
      */
    protected override def failure(context: Context, token:Object) : Unit = {
        monitor.finishJob(token, Status.FAILED)
    }

    /**
      * Marks a run as a failure
      *
      * @param context
      * @param token
      */
    protected override def aborted(context: Context, token:Object) : Unit = {
        monitor.finishJob(token, Status.ABORTED)
    }

    /**
      * Marks a run as being skipped
      *
      * @param context
      * @param token
      */
    protected override def skipped(context: Context, token:Object) : Unit = {
        monitor.finishJob(token, Status.SKIPPED)
    }
}
