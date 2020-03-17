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
import com.dimajix.flowman.spec.relation.HiveTableRelation


case class HiveTable(
    database:Environment => Option[String] = _ => None,
    schema:Option[Template[Schema]] = None,
    partitions: Environment => Seq[PartitionField] = _ => Seq(),
    table:Environment => String,
    external: Environment => Boolean = _ => false,
    location:Environment => Option[Path] = _ => None,
    format: Environment => String = _ => "parquet",
    rowFormat: Environment => Option[String] = _ => None,
    inputFormat: Environment => Option[String] = _ => None,
    outputFormat: Environment => Option[String] = _ => None,
    properties: Environment => Map[String, String] = _ => Map(),
    serdeProperties: Environment => Map[String, String] = _ => Map(),
    writer: Environment => String = _ => "hive"
) extends RelationGen {
    override def apply(props:Relation.Properties) : HiveTableRelation = {
        val env = props.context.environment
        HiveTableRelation(
            props,
            database = database(env),
            schema = schema.map(s => s.instantiate(props.context)),
            table = table(env),
            external = external(env),
            location = location(env),
            format = format(env),
            rowFormat = rowFormat(env),
            inputFormat = inputFormat(env),
            outputFormat = outputFormat(env),
            properties = properties(env),
            serdeProperties = serdeProperties(env),
            writer = writer(env)
        )
    }
}
