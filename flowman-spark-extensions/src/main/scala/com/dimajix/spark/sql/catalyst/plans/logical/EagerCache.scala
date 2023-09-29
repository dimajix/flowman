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

package com.dimajix.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan


case class EagerCache(child: LogicalPlan, caches: Seq[LogicalPlan]) extends LogicalPlan {
    override def children: Seq[LogicalPlan] = child +: caches
    override def maxRows: Option[Long] = child.maxRows
    override def output: Seq[Attribute] = child.output

    override protected def doCanonicalize(): LogicalPlan = child.canonicalized
        //copy(child=child.canonicalized, caches=caches.map(_.canonicalized.asInstanceOf[InMemoryRelation]))

    /*override*/ protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan =
        copy(child=newChildren.head, caches=newChildren.tail)
}
