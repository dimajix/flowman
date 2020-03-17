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

package com.dimajix.flowman.dsl.relation

import com.dimajix.flowman.dsl.RelationGen
import com.dimajix.flowman.execution.Environment
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.spec.relation.HiveViewRelation


case class HiveView(
    database: Environment => Option[String] = _ => None,
    view: Environment => String,
    partitions: Environment => Seq[PartitionField] = _ => Seq(),
    sql: Environment => Option[String] = _ => None,
    mapping: Environment => Option[MappingOutputIdentifier] = _ => None
) extends RelationGen {
    override def apply(props: Relation.Properties): HiveViewRelation = {
        val env = props.context.environment
        HiveViewRelation(
            props,
            database(env),
            view(env),
            partitions(env),
            sql(env),
            mapping(env)
        )
    }
}
