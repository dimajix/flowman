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
  * @param name
  * @param status
  * @param jobName
  * @param jobId
  * @param from
  * @param to
  * @param partitions
  */
case class TargetQuery(
    namespace:Option[String] = None,
    project:Option[String] = None,
    name:Option[String] = None,
    status:Option[Status] = None,
    phase:Option[Phase] = None,
    jobName:Option[String] = None,
    jobId:Option[String] = None,
    from:Option[ZonedDateTime] = None,
    to:Option[ZonedDateTime] = None,
    partitions:Map[String,String] = Map()
)


case class TargetState(
    id:String,
    jobId:Option[String],
    namespace:String,
    project:String,
    target:String,
    partitions:Map[String,String],
    phase:Phase,
    status:Status,
    startDateTime:Option[ZonedDateTime] = None,
    endDateTime:Option[ZonedDateTime] = None
)


sealed case class TargetOrder (isAscending:Boolean=true) {
    def asc() : TargetOrder  = copy(true)
    def desc() : TargetOrder  = copy(false)
}

object TargetOrder {
    object BY_DATETIME extends TargetOrder
    object BY_NAME extends TargetOrder
    object BY_ID extends TargetOrder
    object BY_STATUS extends TargetOrder
    object BY_PHASE extends TargetOrder
    object BY_PARENT_NAME extends TargetOrder
    object BY_PARENT_ID extends TargetOrder
}
