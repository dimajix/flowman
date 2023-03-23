/*
 * Copyright (C) 2021 The Flowman Authors
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

package com.dimajix.flowman.execution

import org.apache.spark.sql.Column


sealed abstract class MergeClause extends Product with Serializable {
    def condition : Option[Column]
}

sealed abstract class MergeMatchedClause extends MergeClause
sealed abstract class MergeUnmatchedClause extends MergeClause

case class InsertClause(
    condition: Option[Column] = None,
    columns:Map[String,Column] = Map()
) extends MergeUnmatchedClause

case class DeleteClause(
    condition: Option[Column] = None
) extends MergeMatchedClause

case class UpdateClause(
    condition: Option[Column] = None,
    columns:Map[String,Column] = Map()
) extends MergeMatchedClause
