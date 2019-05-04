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

import com.dimajix.flowman.state.StateStore
import com.dimajix.flowman.state.JobInstance
import com.dimajix.flowman.state.JobToken
import com.dimajix.flowman.state.Status
import com.dimajix.flowman.state.TargetInstance
import com.dimajix.flowman.state.TargetToken


/**
  * This implementation of the Runner interface provides monitoring via calling appropriate methods in
  * a StateStoreProvider
  *
  * @param stateStore
  */
class MonitoredRunner(stateStore: StateStore, parentJob:Option[JobToken] = None) extends AbstractRunner {
    override protected val logger = LoggerFactory.getLogger(classOf[MonitoredRunner])

    override protected def jobRunner(job:JobToken) : Runner = {
        new MonitoredRunner(stateStore, Some(job))
    }

    /**
      * Performs some checkJob, if the run is required
      *
      * @param job
      * @return
      */
    protected override def checkJob(context: Context, job: JobInstance): Boolean = {
        stateStore.checkJob(job)
    }

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @param job
      * @return
      */
    protected override def startJob(context: Context, job: JobInstance, parent: Option[JobToken]): JobToken = {
        stateStore.startJob(job, parent)
    }

    /**
      * Marks a run as a success
      *
      * @param token
      */
    protected override def finishJob(context: Context, token: JobToken): Unit = {
        stateStore.finishJob(token, Status.SUCCESS)
    }

    /**
      * Marks a run as a failure
      *
      * @param token
      */
    protected override def failJob(context: Context, token: JobToken): Unit = {
        stateStore.finishJob(token, Status.FAILED)
    }

    /**
      * Marks a run as a failure
      *
      * @param context
      * @param token
      */
    protected override def abortJob(context: Context, token: JobToken): Unit = {
        stateStore.finishJob(token, Status.ABORTED)
    }

    /**
      * Marks a run as being skipped
      *
      * @param context
      * @param token
      */
    protected override def skipJob(context: Context, token: JobToken): Unit = {
        stateStore.finishJob(token, Status.SKIPPED)
    }

    /**
      * Performs some checks, if the run is required. Returns faöse if the target is out of date needs to be rebuilt
      *
      * @param target
      * @return
      */
    protected override def checkTarget(context: Context, target: TargetInstance): Boolean = {
        stateStore.checkTarget(target)
    }

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @param target
      * @return
      */
    protected override def startTarget(context: Context, target: TargetInstance, parent: Option[JobToken]): TargetToken = {
        stateStore.startTarget(target, parent)
    }

    /**
      * Marks a run as a success
      *
      * @param token
      */
    protected override def finishTarget(context: Context, token: TargetToken): Unit = {
        stateStore.finishTarget(token, Status.SUCCESS)
    }

    /**
      * Marks a run as a failure
      *
      * @param token
      */
    protected override def failTarget(context: Context, token: TargetToken): Unit = {
        stateStore.finishTarget(token, Status.FAILED)
    }

    /**
      * Marks a run as being skipped
      *
      * @param context
      * @param token
      */
    protected override def skipTarget(context: Context, token: TargetToken): Unit = {
        stateStore.finishTarget(token, Status.SKIPPED)
    }
}
