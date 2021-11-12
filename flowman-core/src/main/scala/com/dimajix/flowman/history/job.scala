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
    id:Option[String] = None,
    namespace:Option[String] = None,
    project:Option[String] = None,
    job:Option[String] = None,
    status:Option[Status] = None,
    phase:Option[Phase] = None,
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
    endDateTime:Option[ZonedDateTime] = None
)


sealed case class JobOrderColumn()
object JobOrderColumn {
    object BY_DATETIME extends JobOrderColumn
    object BY_NAME extends JobOrderColumn
    object BY_ID extends JobOrderColumn
    object BY_STATUS extends JobOrderColumn
    object BY_PHASE extends JobOrderColumn
}

object JobOrder {
    final val BY_DATETIME = JobOrder(JobOrderColumn.BY_DATETIME)
    final val BY_NAME = JobOrder(JobOrderColumn.BY_NAME)
    final val BY_ID = JobOrder(JobOrderColumn.BY_ID)
    final val BY_STATUS = JobOrder(JobOrderColumn.BY_STATUS)
    final val BY_PHASE = JobOrder(JobOrderColumn.BY_PHASE)
}
final case class JobOrder(column:JobOrderColumn, isAscending:Boolean=true) {
    def asc() : JobOrder  = copy(isAscending=true)
    def desc() : JobOrder  = copy(isAscending=false)
}
