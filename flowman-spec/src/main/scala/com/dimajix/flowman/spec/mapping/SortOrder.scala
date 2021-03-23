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

package com.dimajix.flowman.spec.mapping

import java.util.Locale


object NullOrdering {
    def of(str:String) : NullOrdering = {
        val tokens = str.toLowerCase(Locale.ROOT).split(' ').toSeq.map(_.trim).filter(_.nonEmpty)
        tokens match {
            case Seq("nulls", "first") => NullsFirst
            case Seq("nulls", "last") => NullsLast
            case _ => throw new IllegalArgumentException(s"Unsupported null ordering '$str'")
        }
    }
}
abstract sealed class NullOrdering
case object NullsFirst extends NullOrdering
case object NullsLast extends NullOrdering

object SortDirection {
    def of(str:String) : SortDirection = {
        str.toLowerCase(Locale.ROOT) match {
            case "asc" => Ascending
            case "desc" => Descending
            case _ => throw new IllegalArgumentException(s"Unsupported sort direction '$str'")
        }
    }
}
abstract sealed class SortDirection {
    def defaultNullOrdering: NullOrdering
}
case object Ascending extends SortDirection {
    override def defaultNullOrdering: NullOrdering = NullsFirst
}
case object Descending extends SortDirection {
    override def defaultNullOrdering: NullOrdering = NullsLast
}

object SortOrder {
    def apply(direction: SortDirection) : SortOrder = SortOrder(direction, direction.defaultNullOrdering)

    def of(str:String) : SortOrder = {
        val tokens = str.split(' ').map(_.trim).filter(_.nonEmpty)
        tokens.toSeq match {
            case Seq(str) => SortOrder(SortDirection.of(str))
            case Seq(d, n1, n2) => SortOrder(SortDirection.of(d), NullOrdering.of(n1 + " " + n2))
            case _ => throw new IllegalArgumentException(s"Unsupported sort order '$str'")
        }
    }
}
case class SortOrder(direction:SortDirection, nullOrdering: NullOrdering)
