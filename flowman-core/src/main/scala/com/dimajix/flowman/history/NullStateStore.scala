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

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.JobInstance
import com.dimajix.flowman.model.TargetInstance


class NullStateStore extends StateStore {
    /**
      * Returns the state of a batch
      * @param batch
      * @return
      */
    override def getJobState(batch:JobInstance) : Option[JobState] = None

    /**
      * Starts the run and returns a token, which can be anything
      * @param batch
      * @return
      */
    override def startJob(batch:JobInstance, phase:Phase) : JobToken = null

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
    override def getTargetState(target:TargetInstance) : Option[TargetState] = None

    /**
      * Starts the run and returns a token, which can be anything
      * @param target
      * @return
      */
    override def startTarget(target:TargetInstance, phase:Phase, parent:Option[JobToken]=None) : TargetToken = null

    /**
      * Sets the status of a target after it has been started
      * @param token
      * @param status
      */
    override def finishTarget(token:TargetToken, status:Status) : Unit = {}

    /**
      * Returns a list of job matching the query criteria
      * @param query
      * @param limit
      * @param offset
      * @return
      */
    override def findJobs(query:JobQuery, order:Seq[JobOrder], limit:Int, offset:Int) : Seq[JobState] = Seq()

    /**
      * Returns a list of job matching the query criteria
      * @param query
      * @param limit
      * @param offset
      * @return
      */
    override def findTargets(query:TargetQuery, order:Seq[TargetOrder], limit:Int, offset:Int) : Seq[TargetState] = Seq()
}
