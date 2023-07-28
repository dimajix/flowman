/*
 * Copyright (C) 2023 The Flowman Authors
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

package com.dimajix.flowman.spec.history

import com.dimajix.flowman.documentation.EntityDoc
import com.dimajix.flowman.documentation.ProjectDoc
import com.dimajix.flowman.history.DocumentationQuery
import com.dimajix.flowman.history.Graph
import com.dimajix.flowman.history.JobColumn
import com.dimajix.flowman.history.JobOrder
import com.dimajix.flowman.history.JobQuery
import com.dimajix.flowman.history.JobState
import com.dimajix.flowman.history.Measurement
import com.dimajix.flowman.history.MetricSeries
import com.dimajix.flowman.history.TargetColumn
import com.dimajix.flowman.history.TargetOrder
import com.dimajix.flowman.history.TargetQuery
import com.dimajix.flowman.history.TargetState
import com.dimajix.flowman.model.JobDigest
import com.dimajix.flowman.model.TargetDigest


abstract class StateRepository {
    def create() : Unit

    def withRetry[T](fn: => T) : T

    def getJobState(job:JobDigest) : Option[JobState]
    def insertJobState(state:JobState, env:Map[String,String]) : JobState
    def updateJobState(state:JobState) : Unit
    def insertJobMetrics(jobId:String, metrics:Seq[Measurement]) : Unit
    def getJobMetrics(jobId:String) : Seq[Measurement]
    def getJobEnvironment(jobId:String) : Map[String,String]
    def insertJobGraph(jobId:String, graph:Graph) : Unit
    def getJobGraph(jobId:String) : Option[Graph]
    def insertJobDocumentation(jobId:String, doc:ProjectDoc) : Unit
    def getJobDocumentation(jobId:String) : Option[ProjectDoc]
    def findJobs(query:JobQuery, order:Seq[JobOrder]=Seq.empty, limit:Int=10000, offset:Int=0) : Seq[JobState]
    def countJobs(query:JobQuery) : Int
    def countJobs(query:JobQuery, grouping:JobColumn) : Seq[(String,Int)]

    def getTargetState(target:TargetDigest) : Option[TargetState]
    def getTargetState(targetId:String) : TargetState
    def insertTargetState(state:TargetState) : TargetState
    def updateTargetState(state:TargetState) : Unit
    def findTargets(query:TargetQuery, order:Seq[TargetOrder]=Seq.empty, limit:Int=10000, offset:Int=0) : Seq[TargetState]
    def countTargets(query:TargetQuery) : Int
    def countTargets(query:TargetQuery, grouping:TargetColumn) : Seq[(String,Int)]

    def findMetrics(query: JobQuery, groupings:Seq[String]) : Seq[MetricSeries]
    def findDocumentation(query:DocumentationQuery) : Seq[EntityDoc]
}
