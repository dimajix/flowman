/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobDigest
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetResult


object NullStateStore {
    private case class DummyJobToken() extends JobToken
    private case class DummyTargetToken() extends TargetToken
}


class NullStateStore extends AbstractStateStore {
    import NullStateStore._

    /**
      * Returns the state of a batch
      * @param batch
      * @return
      */
    override def getJobState(batch:JobDigest) : Option[JobState] = None

    /**
     * Returns all metrics belonging to a specific job instance
     * @param jobId
     * @return
     */
    override def getJobMetrics(jobId:String) : Seq[Measurement] = Seq()

    /**
     * Returns the execution graph belonging to a specific job run
     *
     * @param jobId
     * @return
     */
    override def getJobGraph(jobId: String): Option[Graph] = None

    /**
     * Returns the execution environment of a specific job run
     *
     * @param jobId
     * @return
     */
    override def getJobEnvironment(jobId: String): Map[String, String] = Map()

    /**
      * Starts the run and returns a token, which can be anything
     *
     * @param batch
      * @return
      */
    override def startJob(job:Job, digest:JobDigest) : JobToken = DummyJobToken()

    /**
      * Sets the status of a job after it has been started
      * @param token
      * @param status
      */
    override def finishJob(token:JobToken, result:JobResult, metrics:Seq[Measurement]=Seq()) : Unit = {}

    /**
      * Returns the state of a target
      * @param target
      * @return
      */
    override def getTargetState(target:TargetDigest) : Option[TargetState] = None
    override def getTargetState(targetId: String): TargetState = ???

    /**
      * Starts the run and returns a token, which can be anything
      * @param target
      * @return
      */
    override def startTarget(target:Target, digest:TargetDigest, parent:Option[JobToken]=None) : TargetToken = DummyTargetToken()

    /**
      * Sets the status of a target after it has been started
      * @param token
      * @param status
      */
    override def finishTarget(token:TargetToken, result:TargetResult) : Unit = {}

    /**
      * Returns a list of job matching the query criteria
      * @param query
      * @param limit
      * @param offset
      * @return
      */
    override def findJobs(query:JobQuery, order:Seq[JobOrder]=Seq(), limit:Int=10000, offset:Int=0) : Seq[JobState] = Seq()

    override def countJobs(query:JobQuery) : Int = 0
    override def countJobs(query:JobQuery, grouping:JobColumn) : Map[String,Int] = Map()

    /**
      * Returns a list of job matching the query criteria
      * @param query
      * @param limit
      * @param offset
      * @return
      */
    override def findTargets(query:TargetQuery, order:Seq[TargetOrder]=Seq(), limit:Int=10000, offset:Int=0) : Seq[TargetState] = Seq()

    override def countTargets(query: TargetQuery): Int = 0
    override def countTargets(query:TargetQuery, grouping:TargetColumn) : Map[String,Int] = Map()

    override def findJobMetrics(jobQuery: JobQuery, groupings: Seq[String]): Seq[MetricSeries] = Seq()
}
