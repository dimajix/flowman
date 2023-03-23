/*
 * Copyright (C) 2018 The Flowman Authors
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

import java.time.ZonedDateTime

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status


/**
  * The JobQuery object is used to retrieve a list of jobs matching the given critera
  * @param namespace
  * @param project
  * @param job
  * @param status
  * @param from
  * @param to
  * @param args
  */
final case class JobQuery(
    id:Seq[String] = Seq(),
    namespace:Seq[String] = Seq(),
    project:Seq[String] = Seq(),
    job:Seq[String] = Seq(),
    status:Seq[Status] = Seq(),
    phase:Seq[Phase] = Seq(),
    from:Option[ZonedDateTime] = None,
    to:Option[ZonedDateTime] = None,
    args:Map[String,String] = Map()
)


final case class JobState(
    id:String,
    namespace:String,
    project:String,
    version:String,
    job:String,
    phase:Phase,
    args:Map[String,String],
    status:Status,
    startDateTime:Option[ZonedDateTime] = None,
    endDateTime:Option[ZonedDateTime] = None,
    error:Option[String] = None
)


sealed abstract class JobColumn
object JobColumn {
    case object DATETIME extends JobColumn
    case object PROJECT extends JobColumn
    case object NAME extends JobColumn
    case object ID extends JobColumn
    case object STATUS extends JobColumn
    case object PHASE extends JobColumn
}

object JobOrder {
    final val BY_DATETIME = JobOrder(JobColumn.DATETIME)
    final val BY_PROJECT = JobOrder(JobColumn.PROJECT)
    final val BY_NAME = JobOrder(JobColumn.NAME)
    final val BY_ID = JobOrder(JobColumn.ID)
    final val BY_STATUS = JobOrder(JobColumn.STATUS)
    final val BY_PHASE = JobOrder(JobColumn.PHASE)
}
final case class JobOrder(column:JobColumn, isAscending:Boolean=true) {
    def asc() : JobOrder  = copy(isAscending=true)
    def desc() : JobOrder  = copy(isAscending=false)
}
