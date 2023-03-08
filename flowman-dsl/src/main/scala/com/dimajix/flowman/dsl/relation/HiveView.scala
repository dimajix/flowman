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

package com.dimajix.flowman.dsl.relation

import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.dsl.RelationGen
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.spec.relation.HiveViewRelation


case class HiveView(
    database: Option[String] = None,
    view: String,
    partitions: Seq[PartitionField] = Seq(),
    sql: Option[String] = None,
    mapping: Option[MappingOutputIdentifier] = None
) extends RelationGen {
    override def apply(props: Relation.Properties): HiveViewRelation = {
        HiveViewRelation(
            props,
            TableIdentifier(view, database),
            partitions,
            sql,
            mapping
        )
    }
}
