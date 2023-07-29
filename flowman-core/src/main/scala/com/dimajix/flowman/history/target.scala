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
    id:Seq[String] = Seq.empty,
    namespace:Seq[String] = Seq.empty,
    project:Seq[String] = Seq.empty,
    target:Seq[String] = Seq.empty,
    status:Seq[Status] = Seq.empty,
    phase:Seq[Phase] = Seq.empty,
    job:Seq[String] = Seq.empty,
    jobId:Seq[String] = Seq.empty,
    from:Option[ZonedDateTime] = None,
    to:Option[ZonedDateTime] = None,
    partitions:Map[String,String] = Map.empty
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
    endDateTime:Option[ZonedDateTime] = None,
    error:Option[String] = None
)


sealed abstract class TargetColumn
object TargetColumn {
    case object DATETIME extends TargetColumn
    case object PROJECT extends TargetColumn
    case object NAME extends TargetColumn
    case object ID extends TargetColumn
    case object STATUS extends TargetColumn
    case object PHASE extends TargetColumn
    case object PARENT_NAME extends TargetColumn
    case object PARENT_ID extends TargetColumn
}

object TargetOrder {
    final val BY_DATETIME = TargetOrder(TargetColumn.DATETIME)
    final val BY_PROJECT = TargetOrder(TargetColumn.PROJECT)
    final val BY_NAME = TargetOrder(TargetColumn.NAME)
    final val BY_ID = TargetOrder(TargetColumn.ID)
    final val BY_STATUS = TargetOrder(TargetColumn.STATUS)
    final val BY_PHASE = TargetOrder(TargetColumn.PHASE)
    final val BY_PARENT_NAME = TargetOrder(TargetColumn.PARENT_NAME)
    final val BY_PARENT_ID = TargetOrder(TargetColumn.PARENT_ID)
}
final case class TargetOrder(column:TargetColumn, isAscending:Boolean=true) {
    def asc() : TargetOrder  = copy(isAscending=true)
    def desc() : TargetOrder  = copy(isAscending=false)
}
