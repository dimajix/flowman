/*
 * Copyright (C) 2019 The Flowman Authors
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
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.util.LongAccumulator


case class CountRecords(child: LogicalPlan, counter:LongAccumulator) extends UnaryNode {
    override def maxRows: Option[Long] = child.maxRows
    override def output: Seq[Attribute] = child.output

    override protected def doCanonicalize(): LogicalPlan = copy(child=child.canonicalized)

    /*override*/ protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = copy(child=newChild)
}
