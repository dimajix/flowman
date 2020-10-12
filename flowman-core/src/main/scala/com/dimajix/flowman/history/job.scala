/*
 * Copyright 2018-2020 Kaya Kupferschmidt
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
  * @param name
  * @param status
  * @param from
  * @param to
  * @param args
  */
case class JobQuery(
    namespace:Option[String] = None,
    project:Option[String] = None,
    name:Option[String] = None,
    status:Option[Status] = None,
    phase:Option[Phase] = None,
    from:Option[ZonedDateTime] = None,
    to:Option[ZonedDateTime] = None,
    args:Map[String,String] = Map()
)


case class JobState(
    id:String,
    namespace:String,
    project:String,
    job:String,
    phase:Phase,
    args:Map[String,String],
    status:Status,
    startDateTime:Option[ZonedDateTime] = None,
    endDateTime:Option[ZonedDateTime] = None
)


sealed case class JobOrder(isAscending:Boolean=true) {
    def asc() : JobOrder = copy(true)
    def desc() : JobOrder = copy(false)
}

object JobOrder {
    object BY_DATETIME extends JobOrder
    object BY_NAME extends JobOrder
    object BY_ID extends JobOrder
    object BY_STATUS extends JobOrder
    object BY_PHASE extends JobOrder
}
