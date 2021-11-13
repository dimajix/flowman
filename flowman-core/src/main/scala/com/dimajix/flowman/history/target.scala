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

import java.time.ZonedDateTime

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status


/**
  * The TargetQuery encapsulates a query for retrieving all targets matching the given criteria
  * @param namespace
  * @param project
  * @param target
  * @param status
  * @param job
  * @param jobId
  * @param from
  * @param to
  * @param partitions
  */
final case class TargetQuery(
    id:Option[String] = None,
    namespace:Option[String] = None,
    project:Option[String] = None,
    target:Option[String] = None,
    status:Option[Status] = None,
    phase:Option[Phase] = None,
    job:Option[String] = None,
    jobId:Option[String] = None,
    from:Option[ZonedDateTime] = None,
    to:Option[ZonedDateTime] = None,
    partitions:Map[String,String] = Map()
)


final case class TargetState(
    id:String,
    jobId:Option[String],
    namespace:String,
    project:String,
    version:String,
    target:String,
    partitions:Map[String,String],
    phase:Phase,
    status:Status,
    startDateTime:Option[ZonedDateTime] = None,
    endDateTime:Option[ZonedDateTime] = None
)


sealed abstract class TargetOrderColumn
object TargetOrderColumn {
    case object BY_DATETIME extends TargetOrderColumn
    case object BY_NAME extends TargetOrderColumn
    case object BY_ID extends TargetOrderColumn
    case object BY_STATUS extends TargetOrderColumn
    case object BY_PHASE extends TargetOrderColumn
    case object BY_PARENT_NAME extends TargetOrderColumn
    case object BY_PARENT_ID extends TargetOrderColumn
}

object TargetOrder {
    final val BY_DATETIME = TargetOrder(TargetOrderColumn.BY_DATETIME)
    final val BY_NAME = TargetOrder(TargetOrderColumn.BY_NAME)
    final val BY_ID = TargetOrder(TargetOrderColumn.BY_ID)
    final val BY_STATUS = TargetOrder(TargetOrderColumn.BY_STATUS)
    final val BY_PHASE = TargetOrder(TargetOrderColumn.BY_PHASE)
    final val BY_PARENT_NAME = TargetOrder(TargetOrderColumn.BY_PARENT_NAME)
    final val BY_PARENT_ID = TargetOrder(TargetOrderColumn.BY_PARENT_ID)
}
final case class TargetOrder(column:TargetOrderColumn, isAscending:Boolean=true) {
    def asc() : TargetOrder  = copy(isAscending=true)
    def desc() : TargetOrder  = copy(isAscending=false)
}
