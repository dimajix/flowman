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

import com.dimajix.flowman.history.StateStore
import com.dimajix.flowman.history.BatchInstance
import com.dimajix.flowman.history.BatchToken
import com.dimajix.flowman.history.TargetInstance
import com.dimajix.flowman.history.TargetToken


/**
  * This implementation of the Runner interface provides monitoring via calling appropriate methods in
  * a StateStoreSpec
  *
  * @param stateStore
  */
class MonitoredRunner(stateStore: StateStore, parentJob:Option[BatchToken] = None) extends AbstractRunner {
    override protected val logger = LoggerFactory.getLogger(classOf[MonitoredRunner])

    override protected def jobRunner(job:BatchToken) : Runner = {
        new MonitoredRunner(stateStore, Some(job))
    }

    /**
      * Performs some checkJob, if the run is required
      *
      * @param job
      * @return
      */
    protected override def checkJob(job: BatchInstance): Boolean = {
        stateStore.checkJob(job)
    }

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @param job
      * @return
      */
    protected override def startBundle(job: BatchInstance, parent: Option[BatchToken]): BatchToken = {
        stateStore.startBatch(job, parent)
    }

    /**
      * Marks a run as a success
      *
      * @param token
      */
    protected override def finishBatch(token: BatchToken, status:Status): Unit = {
        stateStore.finishBatch(token, status)
    }

    /**
      * Performs some checks, if the run is required. Returns faöse if the target is out of date needs to be rebuilt
      *
      * @param target
      * @return
      */
    protected override def checkTarget(target: TargetInstance, phase: Phase): Boolean = {
        stateStore.checkTarget(target, phase)
    }

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @param target
      * @return
      */
    protected override def startTarget(target: TargetInstance, phase: Phase, parent: Option[BatchToken]): TargetToken = {
        stateStore.startTarget(target, phase, parent)
    }

    /**
      * Marks a run as a success
      *
      * @param token
      */
    protected override def finishTarget(token: TargetToken, status:Status): Unit = {
        stateStore.finishTarget(token, status)
    }
}
