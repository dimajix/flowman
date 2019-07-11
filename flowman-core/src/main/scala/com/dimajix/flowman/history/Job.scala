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

/**
  * A JobInstance serves as an identifier of a specific job in the History
  * @param namespace
  * @param project
  * @param job
  * @param args
  */
case class JobInstance(
    namespace:String,
    project:String,
    job:String,
    args:Map[String,String] = Map()
) {
    require(namespace != null)
    require(project != null)
    require(job != null)
    require(args != null)
}


/**
  * The JobQuery object is used to retrieve a list of jobs matching the given critera
  * @param namespace
  * @param project
  * @param name
  * @param status
  * @param parentName
  * @param parentId
  * @param from
  * @param to
  * @param args
  */
case class JobQuery(
    namespace:Option[String],
    project:Option[String],
    name:Option[String],
    status:Option[Status],
    parentName:Option[String],
    parentId:Option[String],
    from:Option[ZonedDateTime] = None,
    to:Option[ZonedDateTime] = None,
    args:Map[String,String] = Map()
)


case class JobState(
    id:String,
    parentId:Option[String],
    namespace:String,
    project:String,
    job:String,
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
    object BY_PARENT_NAME extends JobOrder
    object BY_PARENT_ID extends JobOrder
}
