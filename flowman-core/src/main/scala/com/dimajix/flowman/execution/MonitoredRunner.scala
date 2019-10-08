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
import com.dimajix.flowman.history.JobToken
import com.dimajix.flowman.history.TargetState
import com.dimajix.flowman.history.TargetToken
import com.dimajix.flowman.spec.job.JobInstance
import com.dimajix.flowman.spec.target.TargetInstance


/**
  * This implementation of the Runner interface provides monitoring via calling appropriate methods in
  * a StateStoreSpec
  *
  * @param stateStore
  */
class MonitoredRunner(stateStore: StateStore) extends AbstractRunner {
    override protected val logger = LoggerFactory.getLogger(classOf[MonitoredRunner])

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @param batch
      * @return
      */
    protected override def startJob(batch: JobInstance, phase: Phase): JobToken = {
        stateStore.startJob(batch, phase)
    }

    /**
      * Marks a run as a success
      *
      * @param token
      */
    protected override def finishJob(token: JobToken, status:Status): Unit = {
        stateStore.finishJob(token, status)
    }

    /**
      * Performs some checks, if the run is required. Returns faöse if the target is out of date needs to be rebuilt
      *
      * @param target
      * @return
      */
    protected override def checkTarget(target: TargetInstance, phase: Phase): Boolean = {
        def checkState(state:TargetState) : Boolean = {
            val lifecycle = Lifecycle.ofPhase(phase)
            if (!lifecycle.contains(state.phase))
                // Different lifecycle => target is not valid
                false
            else if (lifecycle.indexOf(state.phase) < lifecycle.indexOf(phase))
                // Same lifecycle, but previous phase => target is not valid
                false
            else
                state.status == Status.SUCCESS || state.status == Status.SKIPPED
        }

        stateStore.getTargetState(target) match {
            case Some(state:TargetState) => checkState(state)
            case _ => false
        }
    }

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @param target
      * @return
      */
    protected override def startTarget(target: TargetInstance, phase: Phase, parent: Option[JobToken]): TargetToken = {
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
