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

package org.apache.spark.sql.catalyst.plans.logical

object With {
    def unapply(p: LogicalPlan) : Option[(LogicalPlan, Seq[(String, SubqueryAlias)], Boolean)] = {
        p match {
            case uw: UnresolvedWith =>
                val cteRelations = uw.cteRelations.asInstanceOf[Seq[Product]].map { cteRelation =>
                    (
                        cteRelation.productElement(0).asInstanceOf[String],
                        cteRelation.productElement(1).asInstanceOf[SubqueryAlias]
                    )
                }
                Some((uw.child, cteRelations, uw.allowRecursion))
            case _ => None
        }
    }

    def apply(child:LogicalPlan, cteRelations:Seq[(String, SubqueryAlias)], allowRecursion:Boolean) : UnresolvedWith = {
        val spark41CteRelations = cteRelations.map { case (name, alias) => (name, alias, None) }
        val constructorCteRelations =
            if (com.dimajix.spark.SPARK_VERSION_MAJOR > 4 || com.dimajix.spark.SPARK_VERSION_MINOR >= 1)
                spark41CteRelations
            else
                cteRelations

        val module = Class.forName("org.apache.spark.sql.catalyst.plans.logical.UnresolvedWith$")
            .getField("MODULE$")
            .get(null)
        module.getClass
            .getMethod("apply", classOf[LogicalPlan], classOf[Seq[_]], java.lang.Boolean.TYPE)
            .invoke(module, child, constructorCteRelations, Boolean.box(allowRecursion))
            .asInstanceOf[UnresolvedWith]
    }
}
