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

import org.apache.hadoop.fs.Path

import com.dimajix.flowman.dsl.RelationGen
import com.dimajix.flowman.execution.Environment
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.spec.relation.HiveUnionTableRelation


case class HiveUnionTable(
    schema:Option[Template[Schema]] = None,
    partitions: Environment => Seq[PartitionField] = _ => Seq(),
    tableDatabase: Environment => Option[String] = _ => None,
    tablePrefix: Environment => String,
    locationPrefix: Environment => Option[Path] = _ => None,
    viewDatabase: Environment => Option[String] = _ => None,
    view: Environment => String,
    external: Environment => Boolean = _ => false,
    format: Environment => String = _ => "parquet",
    rowFormat: Environment => Option[String] = _ => None,
    inputFormat: Environment => Option[String] = _ => None,
    outputFormat: Environment => Option[String] = _ => None,
    properties: Environment => Map[String, String] = _ => Map(),
    serdeProperties: Environment => Map[String, String] = _ => Map()
) extends RelationGen {
    override def apply(props:Relation.Properties) : HiveUnionTableRelation = {
        val env = props.context.environment
        ???
    }
}
