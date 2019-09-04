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
  * A JobInstance serves as an identifier of a specific job in the History
  * @param namespace
  * @param project
  * @param job
  * @param args
  */
case class BundleInstance(
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
case class BundleQuery(
    namespace:Option[String],
    project:Option[String],
    name:Option[String],
    status:Option[Status],
    phase:Option[Phase],
    from:Option[ZonedDateTime] = None,
    to:Option[ZonedDateTime] = None,
    args:Map[String,String] = Map()
)


case class BundleState(
    id:String,
    namespace:String,
    project:String,
    bundle:String,
    phase: String,
    args:Map[String,String],
    status:Status,
    startDateTime:Option[ZonedDateTime] = None,
    endDateTime:Option[ZonedDateTime] = None
)


sealed case class BundleOrder(isAscending:Boolean=true) {
    def asc() : BundleOrder = copy(true)
    def desc() : BundleOrder = copy(false)
}

object BundleOrder {
    object BY_DATETIME extends BundleOrder
    object BY_NAME extends BundleOrder
    object BY_ID extends BundleOrder
    object BY_STATUS extends BundleOrder
    object BY_PHASE extends BundleOrder
}
